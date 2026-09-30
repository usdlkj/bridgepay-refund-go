package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
)

var (
	ErrDuplicateRefund = errors.New("duplicate refund")
	ErrNotFound        = errors.New("not found")
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func NewID() string { return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String() }

type RefundInsert struct {
	ID             string
	RefundGANumber string
	Status         RefundStatus
	Amount         string
	AmountData     json.RawMessage
	Data           json.RawMessage
	Reason         string
	BankData       json.RawMessage
	RequestData    json.RawMessage
	BankDataID     *string
	BankDataCreate *BankDataInsert
	Detail         *RefundDetailInsert
}

type RefundDetailInsert struct {
	ID, RefundID, Email, PhoneNumber, Reason, RefundGANumber, Amount, TicketOffice string
	Tickets                                                                        []RefundTicketInsert
}

type RefundTicketInsert struct {
	ID, ArrivalStation, CarsNumber, DepartureStation, IdentityNumberEncrypted string
	IdentityType, Name, OrderNumber, PurchasePrice, SeatNumber                string
	TicketClass, TicketNumber                                                 string
	DepartureDate                                                             *time.Time
}

type BankDataInsert struct {
	ID                string
	BankCode          string
	AccountNumberEnc  json.RawMessage
	AccountNumberHash string
	Status            AccountStatus
	Result            AccountResult
	LastCheckAt       time.Time
}

func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CreateRefund writes the detail, tickets, and refund atomically. The unique
// Node-owned refund_ga_number index is the concurrency authority: a racing
// request receives ErrDuplicateRefund and no partial detail/ticket rows commit.
func (s *Store) CreateRefund(ctx context.Context, input RefundInsert) (Refund, error) {
	if strings.TrimSpace(input.RefundGANumber) == "" {
		return Refund{}, errors.New("refund GA number is required")
	}
	if input.Status == "" {
		return Refund{}, errors.New("refund status is required")
	}
	for name, value := range map[string]json.RawMessage{
		"refund data": input.Data, "refund bank data": input.BankData, "request data": input.RequestData,
	} {
		if err := ValidateSanitizedJSON(value); err != nil {
			return Refund{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	if input.ID == "" {
		input.ID = NewID()
	}
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		if input.BankDataID == nil && input.BankDataCreate != nil {
			record, _, err := getOrCreateBankDataTx(ctx, tx, *input.BankDataCreate)
			if err != nil {
				return fmt.Errorf("create refund bank data: %w", err)
			}
			input.BankDataID = &record.ID
		}
		var detailID *string
		if input.Detail != nil {
			if input.Detail.ID == "" {
				input.Detail.ID = NewID()
			}
			detailID = &input.Detail.ID
			if _, err := tx.Exec(ctx, `
				INSERT INTO refund_details
				(id, refund_id, email, phone_number, reason, refund_ga_number, refund_amount, ticket_office)
				VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,'')::numeric,$8)`,
				input.Detail.ID, nullIfEmpty(input.Detail.RefundID), nullIfEmpty(input.Detail.Email),
				nullIfEmpty(input.Detail.PhoneNumber), nullIfEmpty(input.Detail.Reason),
				nullIfEmpty(input.Detail.RefundGANumber), input.Detail.Amount, nullIfEmpty(input.Detail.TicketOffice)); err != nil {
				return fmt.Errorf("insert refund detail: %w", err)
			}
			for i := range input.Detail.Tickets {
				ticket := &input.Detail.Tickets[i]
				if ticket.ID == "" {
					ticket.ID = NewID()
				}
				if ticket.IdentityNumberEncrypted == "" && ticket.IdentityType != "" {
					return errors.New("ticket identity number must be encrypted before persistence")
				}
				if ticket.IdentityNumberEncrypted != "" {
					if err := validateCiphertext(json.RawMessage(ticket.IdentityNumberEncrypted)); err != nil {
						return errors.New("ticket identity number must use the Encryptor ciphertext format")
					}
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO refund_detail_tickets
					(id, arrival_station, cars_number, departure_date, departure_station, identity_number,
					 identity_type, name, order_number, purchase_price, seat_number, ticket_class,
					 ticket_number, refund_detail_id)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,'')::numeric,$11,$12,$13,$14)`,
					ticket.ID, nullIfEmpty(ticket.ArrivalStation), nullIfEmpty(ticket.CarsNumber), ticket.DepartureDate,
					nullIfEmpty(ticket.DepartureStation), nullIfEmpty(ticket.IdentityNumberEncrypted),
					nullIfEmpty(ticket.IdentityType), nullIfEmpty(ticket.Name), nullIfEmpty(ticket.OrderNumber),
					ticket.PurchasePrice, nullIfEmpty(ticket.SeatNumber), nullIfEmpty(ticket.TicketClass),
					nullIfEmpty(ticket.TicketNumber), input.Detail.ID); err != nil {
					return fmt.Errorf("insert refund ticket: %w", err)
				}
			}
		}

		command, err := tx.Exec(ctx, `
			INSERT INTO refunds
			(id, refund_ga_number, refund_status, refund_amount, refund_amount_data, refund_data,
			 refund_reason, refund_bank_data, request_data, refund_detail_id, bank_data_id)
			VALUES ($1,$2,$3,NULLIF($4,'')::numeric,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (refund_ga_number) DO NOTHING`,
			input.ID, input.RefundGANumber, input.Status, input.Amount, jsonOrNil(input.AmountData),
			jsonOrNil(input.Data), nullIfEmpty(input.Reason), jsonOrNil(input.BankData),
			jsonOrNil(input.RequestData), detailID, input.BankDataID)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrDuplicateRefund
			}
			return fmt.Errorf("insert refund: %w", err)
		}
		if command.RowsAffected() == 0 {
			return ErrDuplicateRefund
		}
		return nil
	})
	if err != nil {
		return Refund{}, err
	}
	return s.GetRefundByGANumber(ctx, input.RefundGANumber)
}

func (s *Store) GetRefundByGANumber(ctx context.Context, refundGANumber string) (Refund, error) {
	return scanRefund(s.pool.QueryRow(ctx, refundSelect+` WHERE refund_ga_number = $1`, refundGANumber))
}

func (s *Store) LockRefundByGANumber(ctx context.Context, tx pgx.Tx, refundGANumber string) (Refund, error) {
	return scanRefund(tx.QueryRow(ctx, refundSelect+` WHERE refund_ga_number = $1 FOR UPDATE`, refundGANumber))
}

const refundSelectColumns = `id, refund_ga_number, refund_status, refund_amount::text,
	refund_amount_data, refund_data, refund_reason, reject_reason, reject_by, approval_fin_by,
	approval_rbd_by, approval_fin_at, approval_rbd_at, reject_at, refund_bank_data, refund_date,
	request_data, retry_attempt, retry_date, target_refund_date, refund_execute_data, notif_log,
	refund_detail_id, disbursement_id, disbursement_response, bank_data_id,
	created_at, updated_at, deleted_at`
const refundSelect = `SELECT ` + refundSelectColumns + ` FROM refunds`

type rowScanner interface{ Scan(...any) error }

func scanRefund(row rowScanner) (Refund, error) {
	var record Refund
	err := row.Scan(&record.ID, &record.RefundGANumber, &record.Status, &record.Amount,
		&record.AmountData, &record.Data, &record.Reason, &record.RejectReason, &record.RejectBy,
		&record.ApprovalFinanceBy, &record.ApprovalRBDBy, &record.ApprovalFinanceAt, &record.ApprovalRBDAt,
		&record.RejectAt, &record.BankData, &record.RefundDate, &record.RequestData, &record.RetryAttempt,
		&record.RetryDate, &record.TargetRefundDate, &record.ExecuteData, &record.NotificationLog,
		&record.RefundDetailID, &record.DisbursementID, &record.DisbursementResponse, &record.BankDataID,
		&record.CreatedAt, &record.UpdatedAt, &record.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Refund{}, ErrNotFound
	}
	return record, err
}

// GetOrCreateBankData serializes a competing account check on the existing
// Node unique key without ever writing a plaintext account number.
func (s *Store) GetOrCreateBankData(ctx context.Context, input BankDataInsert) (BankData, bool, error) {
	var record BankData
	var created bool
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		record, created, err = getOrCreateBankDataTx(ctx, tx, input)
		return err
	})
	return record, created, err
}

func getOrCreateBankDataTx(ctx context.Context, tx pgx.Tx, input BankDataInsert) (BankData, bool, error) {
	if input.ID == "" {
		input.ID = NewID()
	}
	if input.Status == "" {
		input.Status = AccountPending
	}
	if input.Result == "" {
		input.Result = AccountResultPending
	}
	if input.LastCheckAt.IsZero() {
		input.LastCheckAt = time.Now()
	}
	if err := validateCiphertext(input.AccountNumberEnc); err != nil {
		return BankData{}, false, err
	}
	row := tx.QueryRow(ctx, `
			INSERT INTO bank_datas
			(id, bank_code, account_number_enc, account_number_hash, account_status, account_result, last_check_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (bank_code, account_number_hash) DO NOTHING
			RETURNING id, bank_code, account_number_enc, account_number_hash, request_id,
			 account_status, account_result, iluma_data, failure_code, failure_message,
			 last_check_at, created_at, updated_at, deleted_at`,
		input.ID, input.BankCode, input.AccountNumberEnc, input.AccountNumberHash,
		input.Status, input.Result, input.LastCheckAt)
	record, err := scanBankData(row)
	if err == nil {
		return record, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return BankData{}, false, err
	}
	record, err = scanBankData(tx.QueryRow(ctx, bankDataSelect+`
			WHERE bank_code = $1 AND account_number_hash = $2 FOR UPDATE`, input.BankCode, input.AccountNumberHash))
	return record, false, err
}

func (s *Store) GetBankDataByID(ctx context.Context, id string) (BankData, error) {
	return scanBankData(s.pool.QueryRow(ctx, bankDataSelect+` WHERE id = $1`, id))
}

const bankDataSelect = `SELECT id, bank_code, account_number_enc, account_number_hash, request_id,
	account_status, account_result, iluma_data, failure_code, failure_message,
	last_check_at, created_at, updated_at, deleted_at FROM bank_datas`

func scanBankData(row rowScanner) (BankData, error) {
	var record BankData
	err := row.Scan(&record.ID, &record.BankCode, &record.AccountNumberEnc, &record.AccountNumberHash,
		&record.RequestID, &record.AccountStatus, &record.AccountResult, &record.IlumaData,
		&record.FailureCode, &record.FailureMessage, &record.LastCheckAt, &record.CreatedAt,
		&record.UpdatedAt, &record.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return BankData{}, ErrNotFound
	}
	return record, err
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func jsonOrNil(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
