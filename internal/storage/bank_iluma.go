package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type BankFilter struct {
	Column int
	Value  string
}

func (s *Store) ListEnabledRefundBanks(ctx context.Context) ([]RefundBank, error) {
	return s.queryRefundBanks(ctx, ` WHERE bank_status = 'enable' AND iluma_code IS NOT NULL`, nil)
}

func (s *Store) ListRefundBanks(ctx context.Context, filters []BankFilter) ([]RefundBank, error) {
	var clauses []string
	var args []any
	for _, filter := range filters {
		value := strings.TrimSpace(filter.Value)
		if value == "" {
			continue
		}
		args = append(args, value)
		parameter := fmt.Sprintf("$%d", len(args))
		switch filter.Column {
		case 0:
			clauses = append(clauses, `bank_name ILIKE '%' || `+parameter+` || '%'`)
		case 1:
			clauses = append(clauses, `xendit_code ILIKE '%' || `+parameter+` || '%'`)
		case 2:
			clauses = append(clauses, `bank_status = `+parameter)
		default:
			args = args[:len(args)-1]
		}
	}
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	return s.queryRefundBanks(ctx, where, args)
}

func (s *Store) queryRefundBanks(ctx context.Context, where string, args []any) ([]RefundBank, error) {
	rows, err := s.pool.Query(ctx, refundBankSelect+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RefundBank
	for rows.Next() {
		record, err := scanRefundBank(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) GetRefundBankByID(ctx context.Context, id string) (RefundBank, error) {
	return scanRefundBank(s.pool.QueryRow(ctx, refundBankSelect+` WHERE id = $1`, id))
}

func (s *Store) GetRefundBankByXenditCode(ctx context.Context, code string) (RefundBank, error) {
	return scanRefundBank(s.pool.QueryRow(ctx, refundBankSelect+` WHERE xendit_code = $1 LIMIT 1`, code))
}

func (s *Store) UpdateRefundBank(ctx context.Context, id string, status *BankStatus, deletedAt **time.Time) (RefundBank, error) {
	if status == nil && deletedAt == nil {
		return s.GetRefundBankByID(ctx, id)
	}
	command := `UPDATE refund_banks SET updated_at = now()`
	var args []any
	if status != nil {
		args = append(args, *status)
		command += fmt.Sprintf(", bank_status = $%d", len(args))
	}
	if deletedAt != nil {
		args = append(args, *deletedAt)
		command += fmt.Sprintf(", deleted_at = $%d", len(args))
	}
	args = append(args, id)
	command += fmt.Sprintf(" WHERE id = $%d RETURNING ", len(args)) + refundBankColumns
	return scanRefundBank(s.pool.QueryRow(ctx, command, args...))
}

func (s *Store) UpsertXenditRefundBank(ctx context.Context, bankName, code string, data json.RawMessage) (RefundBank, error) {
	var result RefundBank
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "refund-bank:"+code); err != nil {
			return err
		}
		existing, err := scanRefundBank(tx.QueryRow(ctx, refundBankSelect+` WHERE xendit_code = $1 LIMIT 1 FOR UPDATE`, code))
		if err == nil {
			result, err = scanRefundBank(tx.QueryRow(ctx, `UPDATE refund_banks
				SET bank_name=$2, xendit_data=$3, bank_status='disable', updated_at=now()
				WHERE id=$1 RETURNING `+refundBankColumns, existing.ID, bankName, jsonOrNil(data)))
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		result, err = scanRefundBank(tx.QueryRow(ctx, `INSERT INTO refund_banks
			(id,bank_name,xendit_code,xendit_data,bank_status) VALUES ($1,$2,$3,$4,'disable')
			RETURNING `+refundBankColumns, NewID(), bankName, code, jsonOrNil(data)))
		return err
	})
	return result, err
}

func (s *Store) UpdateIlumaRefundBankByName(ctx context.Context, bankName, code string, data json.RawMessage) (bool, error) {
	command, err := s.pool.Exec(ctx, `UPDATE refund_banks SET iluma_code=$2, iluma_data=$3, updated_at=now() WHERE bank_name=$1`, bankName, code, jsonOrNil(data))
	return command.RowsAffected() > 0, err
}

const refundBankColumns = `id, bank_name, xendit_code, xendit_data, iluma_code, iluma_data,
	bank_status, created_at, updated_at, deleted_at`
const refundBankSelect = `SELECT ` + refundBankColumns + ` FROM refund_banks`

func scanRefundBank(row rowScanner) (RefundBank, error) {
	var record RefundBank
	err := row.Scan(&record.ID, &record.BankName, &record.XenditCode, &record.XenditData,
		&record.IlumaCode, &record.IlumaData, &record.Status, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RefundBank{}, ErrNotFound
	}
	return record, err
}

func (s *Store) FindBankData(ctx context.Context, bankCode, accountHash string) (BankData, error) {
	return scanBankData(s.pool.QueryRow(ctx, bankDataSelect+` WHERE bank_code=$1 AND account_number_hash=$2`, bankCode, accountHash))
}

func (s *Store) FindBankDataByRequestID(ctx context.Context, requestID string) (BankData, error) {
	return scanBankData(s.pool.QueryRow(ctx, bankDataSelect+` WHERE request_id=$1 LIMIT 1`, requestID))
}

func (s *Store) SetBankDataRequestID(ctx context.Context, id, requestID string) error {
	command, err := s.pool.Exec(ctx, `UPDATE bank_datas SET request_id=$2, updated_at=now() WHERE id=$1`, id, requestID)
	if err == nil && command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// CompleteBankData enforces pending->completed and completed->completed. An
// expired row is terminal and cannot be revived by polling or callback work.
func (s *Store) CompleteBankData(ctx context.Context, id string, result AccountResult, ilumaData json.RawMessage, failureCode, failureMessage *string) (bool, error) {
	command, err := s.pool.Exec(ctx, `UPDATE bank_datas SET account_status='completed', account_result=$2,
		iluma_data=$3, failure_code=$4, failure_message=$5, last_check_at=now(), updated_at=now()
		WHERE id=$1 AND account_status IN ('pending','completed')`, id, result, jsonOrNil(ilumaData), failureCode, failureMessage)
	return command.RowsAffected() > 0, err
}

func (s *Store) ExpireBankData(ctx context.Context, id string) (bool, error) {
	command, err := s.pool.Exec(ctx, `UPDATE bank_datas SET account_status='expired', account_result='failed',
		last_check_at=now(), updated_at=now() WHERE id=$1 AND account_status='pending'`, id)
	return command.RowsAffected() > 0, err
}

func (s *Store) InsertIlumaCallLog(ctx context.Context, record IlumaCallLog) error {
	if record.ID == "" {
		record.ID = NewID()
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO iluma_call_logs
		(id,url,method,payload,func,request,response) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		record.ID, record.URL, record.Method, jsonOrNil(record.Payload), record.Function,
		jsonOrNil(record.Request), jsonOrNil(record.Response))
	return err
}

func (s *Store) UpdateIlumaCallback(ctx context.Context, requestID string, response json.RawMessage, responseAt time.Time) (bool, error) {
	command, err := s.pool.Exec(ctx, `UPDATE iluma_callbacks SET response=$2,response_at=$3,updated_at=now() WHERE id=(
		SELECT id FROM iluma_callbacks WHERE request_number=$1 LIMIT 1)`, requestID, jsonOrNil(response), responseAt)
	return command.RowsAffected() > 0, err
}
