package storage

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
)

func (s *Store) GetEnabledRefundBankByXenditCode(ctx context.Context, code string) (RefundBank, error) {
	return scanRefundBank(s.pool.QueryRow(ctx, refundBankSelect+` WHERE xendit_code=$1 AND bank_status='enable' LIMIT 1`, code))
}

// AppendRefundRequestIntent persists the masked provider intent before the
// irreversible HTTP request. The stable reference/idempotency key then allows
// an ambiguous result to be reconciled without creating a different payout.
func (s *Store) AppendRefundRequestIntent(ctx context.Context, id string, intent json.RawMessage) (Refund, error) {
	if err := ValidateSanitizedJSON(intent); err != nil {
		return Refund{}, err
	}
	wrapped, err := json.Marshal([]json.RawMessage{intent})
	if err != nil {
		return Refund{}, err
	}
	return scanRefund(s.pool.QueryRow(ctx, `UPDATE refunds SET
		request_data=COALESCE(request_data,'[]'::jsonb) || $2::jsonb, updated_at=now()
		WHERE id=$1 RETURNING `+refundSelectColumns, id, wrapped))
}

func (s *Store) RecordRefundPayoutSuccess(ctx context.Context, id string, payoutID *string, response json.RawMessage) (Refund, error) {
	if err := ValidateSanitizedJSON(response); err != nil {
		return Refund{}, err
	}
	return scanRefund(s.pool.QueryRow(ctx, `UPDATE refunds SET refund_status='pendingDisbursement',
		disbursement_id=$2, disbursement_response=$3, updated_at=now()
		WHERE id=$1 RETURNING `+refundSelectColumns, id, payoutID, jsonOrNil(response)))
}

func (s *Store) RecordRefundPayoutFailure(ctx context.Context, id, reason string, response json.RawMessage) error {
	if err := ValidateSanitizedJSON(response); err != nil {
		return err
	}
	command, err := s.pool.Exec(ctx, `UPDATE refunds SET refund_status='fail', reject_reason=$2,
		disbursement_response=COALESCE($3,disbursement_response), updated_at=now() WHERE id=$1`,
		id, reason, jsonOrNil(response))
	if err == nil && command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) InsertRefundLog(ctx context.Context, record RefundLog) error {
	if record.ID == "" {
		record.ID = NewID()
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO refund_logs(id,type,location,detail,msg,notes)
		VALUES($1,$2,$3,$4,$5,$6)`, record.ID, record.Type, record.Location,
		record.Detail, record.Message, record.Notes)
	return err
}

func (s *Store) LockRefundByID(ctx context.Context, tx pgx.Tx, id string) (Refund, error) {
	return scanRefund(tx.QueryRow(ctx, refundSelect+` WHERE id=$1 FOR UPDATE`, id))
}

func (s *Store) MarkRefundFailedIfPresent(ctx context.Context, refundGANumber, reason string) error {
	command, err := s.pool.Exec(ctx, `UPDATE refunds SET refund_status='fail',reject_reason=$2,updated_at=now()
		WHERE refund_ga_number=$1`, refundGANumber, reason)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RefundExists(ctx context.Context, refundGANumber string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM refunds WHERE refund_ga_number=$1)`, refundGANumber).Scan(&exists)
	return exists, err
}
