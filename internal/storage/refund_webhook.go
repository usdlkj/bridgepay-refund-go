package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	WebhookAccepted        = "accepted"
	WebhookFollowUp        = "follow_up"
	WebhookProcessing      = "processing"
	WebhookCompleted       = "completed"
	WebhookIgnored         = "ignored"
	WebhookDuplicate       = "duplicate"
	WebhookTicketingFailed = "ticketing_failed"
)

type RefundWebhookInsert struct {
	ID, RefundReference, Source, DedupeKey string
	Payload                                json.RawMessage
}

type RefundWebhookWork struct {
	Call    RefundWebhookCall
	Refund  *Refund
	State   string
	Outcome string
}

// InsertRefundWebhookCall records every authenticated callback. An advisory
// transaction lock makes the first row for a provider event the sole work
// item while later deliveries remain visible as duplicate audit rows.
func (s *Store) InsertRefundWebhookCall(ctx context.Context, input RefundWebhookInsert) (string, error) {
	if input.ID == "" {
		input.ID = NewID()
	}
	if err := ValidateSanitizedJSON(input.Payload); err != nil {
		return "", err
	}
	processID := input.ID
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, input.DedupeKey); err != nil {
			return err
		}
		var existingID string
		err := tx.QueryRow(ctx, `SELECT id FROM refund_webhook_calls
			WHERE response->>'dedupeKey'=$1 AND COALESCE(response->>'state','') <> $2
			ORDER BY created_at LIMIT 1`, input.DedupeKey, WebhookDuplicate).Scan(&existingID)
		duplicateOf := ""
		if err == nil {
			processID, duplicateOf = existingID, existingID
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		state := WebhookAccepted
		if duplicateOf != "" {
			state = WebhookDuplicate
		}
		response, _ := json.Marshal(map[string]string{"state": state, "dedupeKey": input.DedupeKey, "duplicateOf": duplicateOf})
		var refundID *string
		var resolvedID string
		if err := tx.QueryRow(ctx, `SELECT id FROM refunds WHERE refund_ga_number=$1
			AND refund_status IN ('pendingDisbursement','retry') LIMIT 1`, input.RefundReference).Scan(&resolvedID); err == nil {
			refundID = &resolvedID
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO refund_webhook_calls
			(id,refund_id,refund_ref,source,payload,response,response_status)
			VALUES($1,$2,$3,$4,$5,$6,202)`, input.ID, refundID, input.RefundReference,
			input.Source, input.Payload, response)
		return err
	})
	return processID, err
}

func (s *Store) PrepareRefundWebhook(ctx context.Context, id, providerStatus, failureCode, disbursementID string, payout json.RawMessage, retryLimit int, retryDelay time.Duration) (RefundWebhookWork, error) {
	var result RefundWebhookWork
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		call, err := scanRefundWebhookCall(tx.QueryRow(ctx, refundWebhookSelect+` WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		result.Call = call
		var state struct {
			State   string `json:"state"`
			Outcome string `json:"outcome"`
		}
		_ = json.Unmarshal(call.Response, &state)
		result.State, result.Outcome = state.State, state.Outcome
		if state.State == WebhookFollowUp {
			if err := updateWebhookState(ctx, tx, id, WebhookProcessing, state.Outcome, 102); err != nil {
				return err
			}
			if call.RefundID != nil {
				refund, err := scanRefund(tx.QueryRow(ctx, refundSelect+` WHERE id=$1 FOR UPDATE`, *call.RefundID))
				if err != nil {
					return err
				}
				result.Refund = &refund
			}
			return nil
		}
		if state.State == WebhookProcessing {
			return nil
		}
		if state.State != WebhookAccepted || call.RefundID == nil {
			if state.State == WebhookAccepted {
				return updateWebhookState(ctx, tx, id, WebhookIgnored, "", 200)
			}
			return nil
		}
		refund, err := scanRefund(tx.QueryRow(ctx, refundSelect+` WHERE id=$1 FOR UPDATE`, *call.RefundID))
		if err != nil {
			return err
		}
		if refund.Status == nil || (*refund.Status != RefundPendingDisbursement && *refund.Status != RefundRetry) {
			return updateWebhookState(ctx, tx, id, WebhookIgnored, "", 200)
		}
		if providerStatus == "" {
			var envelope struct {
				Data json.RawMessage `json:"data"`
			}
			var data struct {
				ID          string `json:"id"`
				Status      string `json:"status"`
				FailureCode string `json:"failure_code"`
			}
			if json.Unmarshal(call.Payload, &envelope) != nil || json.Unmarshal(envelope.Data, &data) != nil {
				return errors.New("invalid stored Xendit callback")
			}
			providerStatus, failureCode, disbursementID, payout = strings.ToLower(data.Status), strings.ToUpper(data.FailureCode), data.ID, envelope.Data
		}
		switch providerStatus {
		case "accepted", "requested", "pending", "queued":
			_, err = tx.Exec(ctx, `UPDATE refunds SET updated_at=now() WHERE id=$1`, refund.ID)
			if err == nil {
				err = updateWebhookState(ctx, tx, id, WebhookCompleted, "pending", 200)
			}
		case "succeeded":
			if err := ValidateSanitizedJSON(payout); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE refunds SET refund_status='success',
				disbursement_id=COALESCE(NULLIF($2,''),disbursement_id), disbursement_response=$3,
				refund_date=now(), updated_at=now() WHERE id=$1`, refund.ID, disbursementID, payout)
			if err == nil {
				err = updateWebhookState(ctx, tx, id, WebhookFollowUp, "success", 102)
			}
		case "failed", "cancelled", "reversed":
			terminal := terminalFailureCode(failureCode) || retryAttempts(refund.RetryAttempt) >= retryLimit
			if terminal {
				_, err = tx.Exec(ctx, `UPDATE refunds SET refund_status='fail', reject_reason=NULLIF($2,''),
					updated_at=now() WHERE id=$1`, refund.ID, failureCode)
				if err == nil {
					err = updateWebhookState(ctx, tx, id, WebhookFollowUp, "fail", 102)
				}
			} else {
				retryAt := time.Now().Add(retryDelay)
				_, err = tx.Exec(ctx, `UPDATE refunds SET refund_status='fail', reject_reason=NULLIF($2,''),
					retry_date=$3, updated_at=now() WHERE id=$1`, refund.ID, failureCode, retryAt)
				if err == nil {
					err = updateWebhookState(ctx, tx, id, WebhookCompleted, "retry_scheduled", 200)
				}
			}
		default:
			err = updateWebhookState(ctx, tx, id, WebhookIgnored, "unknown_status", 200)
		}
		if err != nil {
			return err
		}
		if resultCall, scanErr := scanRefundWebhookCall(tx.QueryRow(ctx, refundWebhookSelect+` WHERE id=$1`, id)); scanErr == nil {
			result.Call = resultCall
			_ = json.Unmarshal(resultCall.Response, &state)
			result.State, result.Outcome = state.State, state.Outcome
		}
		if result.State == WebhookFollowUp {
			if err := updateWebhookState(ctx, tx, id, WebhookProcessing, result.Outcome, 102); err != nil {
				return err
			}
			updated, err := scanRefund(tx.QueryRow(ctx, refundSelect+` WHERE id=$1`, refund.ID))
			if err != nil {
				return err
			}
			result.Refund = &updated
		}
		return nil
	})
	return result, err
}

func (s *Store) ReleaseRefundWebhook(ctx context.Context, id string) error {
	response, _ := json.Marshal(map[string]string{"state": WebhookFollowUp})
	_, err := s.pool.Exec(ctx, `UPDATE refund_webhook_calls SET response=COALESCE(response,'{}'::jsonb) || $2::jsonb,
		response_status=102,updated_at=now() WHERE id=$1 AND response->>'state'=$3`,
		id, response, WebhookProcessing)
	return err
}

func (s *Store) CompleteRefundWebhook(ctx context.Context, id, outcome string, notification json.RawMessage, responseStatus int) error {
	if err := ValidateSanitizedJSON(notification); err != nil {
		return err
	}
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		call, err := scanRefundWebhookCall(tx.QueryRow(ctx, refundWebhookSelect+` WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		var state struct {
			State string `json:"state"`
		}
		_ = json.Unmarshal(call.Response, &state)
		if state.State != WebhookProcessing {
			return nil
		}
		if call.RefundID == nil {
			return updateWebhookState(ctx, tx, id, WebhookIgnored, outcome, responseStatus)
		}
		status := RefundFail
		if outcome == "success" {
			status = RefundDone
		}
		wrapped, _ := json.Marshal([]json.RawMessage{notification})
		if _, err := tx.Exec(ctx, `UPDATE refunds SET refund_status=$2,
			notif_log=COALESCE(notif_log,'[]'::jsonb) || $3::jsonb, updated_at=now() WHERE id=$1`,
			*call.RefundID, status, wrapped); err != nil {
			return err
		}
		return updateWebhookState(ctx, tx, id, WebhookCompleted, outcome, responseStatus)
	})
}

func (s *Store) FailRefundWebhook(ctx context.Context, id, outcome, reason string, responseStatus int) error {
	response, _ := json.Marshal(map[string]any{"state": WebhookTicketingFailed, "outcome": outcome, "message": reason})
	_, err := s.pool.Exec(ctx, `UPDATE refund_webhook_calls SET response=COALESCE(response,'{}'::jsonb) || $2::jsonb,
		response_status=$3,updated_at=now() WHERE id=$1 AND response->>'state' IN ($4,$5)`,
		id, response, responseStatus, WebhookProcessing, WebhookFollowUp)
	return err
}

func (s *Store) InsertTicketingCallLog(ctx context.Context, record TicketingCallLog) error {
	if record.ID == "" {
		record.ID = NewID()
	}
	if err := ValidateSanitizedJSON(record.Payload); err != nil {
		return err
	}
	if err := ValidateSanitizedJSON(record.Response); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO "ticketing-call-logs"(id,refund_number,payload,response)
		VALUES($1,$2,$3,$4)`, record.ID, record.RefundNumber, record.Payload, record.Response)
	return err
}

func (s *Store) ConfigurationValue(ctx context.Context, name string) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx, `SELECT config_value FROM configurations WHERE config_name=$1 LIMIT 1`, name).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return value, err
}

const refundWebhookSelect = `SELECT id,refund_ref,source,payload,response,response_status,refund_id,created_at,updated_at FROM refund_webhook_calls`

func scanRefundWebhookCall(row rowScanner) (RefundWebhookCall, error) {
	var record RefundWebhookCall
	err := row.Scan(&record.ID, &record.RefundReference, &record.Source, &record.Payload, &record.Response,
		&record.ResponseStatus, &record.RefundID, &record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RefundWebhookCall{}, ErrNotFound
	}
	return record, err
}

func updateWebhookState(ctx context.Context, tx pgx.Tx, id, state, outcome string, responseStatus int) error {
	response, _ := json.Marshal(map[string]string{"state": state, "outcome": outcome})
	_, err := tx.Exec(ctx, `UPDATE refund_webhook_calls SET response=COALESCE(response,'{}'::jsonb) || $2::jsonb,
		response_status=$3,updated_at=now() WHERE id=$1`, id, response, responseStatus)
	return err
}

func retryAttempts(raw json.RawMessage) int {
	var attempts []any
	_ = json.Unmarshal(raw, &attempts)
	return len(attempts)
}

func terminalFailureCode(code string) bool {
	switch code {
	case "INVALID_DESTINATION", "REJECTED_BY_BANK", "TRANSFER_ERROR", "EMPTY_ACCOUNT_NAME", "REJECTED_BY_CHANNEL":
		return true
	default:
		return false
	}
}
