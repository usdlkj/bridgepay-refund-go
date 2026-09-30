package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrRetryNotAllowed = errors.New("retry allowed only for fail status")
	ErrRetryLimit      = errors.New("max attempt to retry")
	ErrRetryConflict   = errors.New("refund retry changed concurrently")
)

func (s *Store) ListRefunds(ctx context.Context, filters []RefundFilter) ([]RefundAdminRecord, error) {
	query := refundSelect
	where, args, err := refundWhere(filters)
	if err != nil {
		return nil, err
	}
	if where != "" {
		query += " WHERE " + where
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]RefundAdminRecord, 0)
	for rows.Next() {
		record, err := scanRefund(rows)
		if err != nil {
			return nil, err
		}
		webhooks, err := s.listRefundWebhooks(ctx, record.ID)
		if err != nil {
			return nil, err
		}
		records = append(records, RefundAdminRecord{Refund: record, WebhookCalls: webhooks})
	}
	return records, rows.Err()
}

func refundWhere(filters []RefundFilter) (string, []any, error) {
	conditions := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters)*2)
	jakarta, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		return "", nil, err
	}
	statuses := map[string]RefundStatus{
		"rbdapproval": RefundRBDApproval, "financeapproval": RefundFinanceApproval,
		"pendingdisbursement": RefundPendingDisbursement, "reject": RefundReject,
		"success": RefundSuccess, "fail": RefundFail, "done": RefundDone,
		"onhold": RefundOnHold, "cancel": RefundCancel, "retry": RefundRetry,
		"pendingchecking": RefundPendingChecking,
	}
	for _, filter := range filters {
		if filter.Value == "" {
			continue
		}
		switch filter.Column {
		case 0:
			args = append(args, "%"+filter.Value+"%")
			conditions = append(conditions, fmt.Sprintf("refund_ga_number ILIKE $%d", len(args)))
		case 1:
			args = append(args, filter.Value)
			conditions = append(conditions, fmt.Sprintf("refund_data->'reqData'->'account'->>'name' = $%d", len(args)))
		case 2:
			value, err := strconv.ParseFloat(filter.Value, 64)
			if err != nil {
				return "", nil, fmt.Errorf("invalid refund amount: %w", err)
			}
			args = append(args, value)
			conditions = append(conditions, fmt.Sprintf("refund_amount = $%d", len(args)))
		case 3, 5:
			day, err := time.ParseInLocation("02-01-2006", filter.Value, jakarta)
			if err != nil {
				continue
			}
			args = append(args, day, day.AddDate(0, 0, 1))
			column := "created_at"
			if filter.Column == 5 {
				column = "refund_date"
			}
			conditions = append(conditions, fmt.Sprintf("%s >= $%d AND %s < $%d", column, len(args)-1, column, len(args)))
		case 4:
			status, ok := statuses[strings.ToLower(filter.Value)]
			if !ok {
				conditions = append(conditions, "false")
				continue
			}
			args = append(args, status)
			conditions = append(conditions, fmt.Sprintf("refund_status = $%d", len(args)))
		}
	}
	return strings.Join(conditions, " AND "), args, nil
}

func (s *Store) GetRefundAdmin(ctx context.Context, id string, relationDetail bool) (RefundAdminRecord, error) {
	record, err := scanRefund(s.pool.QueryRow(ctx, refundSelect+` WHERE id=$1`, id))
	if err != nil {
		return RefundAdminRecord{}, err
	}
	detailID := record.RefundDetailID
	if !relationDetail {
		var id string
		err := s.pool.QueryRow(ctx, `SELECT id FROM refund_details WHERE refund_id=$1 LIMIT 1`, record.ID).Scan(&id)
		if err == nil {
			detailID = &id
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return RefundAdminRecord{}, err
		}
	}
	result := RefundAdminRecord{Refund: record}
	if detailID != nil {
		detail, err := scanRefundDetail(s.pool.QueryRow(ctx, refundDetailSelect+` WHERE id=$1`, *detailID))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return RefundAdminRecord{}, err
		}
		if err == nil {
			result.Detail = &detail
			result.Tickets, err = s.listRefundTickets(ctx, detail.ID)
			if err != nil {
				return RefundAdminRecord{}, err
			}
		}
	}
	return result, nil
}

const refundDetailSelect = `SELECT id, refund_id, email, phone_number, reason, refund_ga_number,
	refund_amount::text, ticket_office, created_at, updated_at, deleted_at FROM refund_details`

func scanRefundDetail(row rowScanner) (RefundDetail, error) {
	var record RefundDetail
	err := row.Scan(&record.ID, &record.RefundID, &record.Email, &record.PhoneNumber, &record.Reason,
		&record.RefundGANumber, &record.Amount, &record.TicketOffice, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RefundDetail{}, ErrNotFound
	}
	return record, err
}

func (s *Store) listRefundTickets(ctx context.Context, detailID string) ([]RefundDetailTicket, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, arrival_station, cars_number, departure_station,
		identity_number, identity_type, name, order_number, purchase_price::text, seat_number,
		ticket_class, ticket_number, refund_detail_id, departure_date, created_at, updated_at, deleted_at
		FROM refund_detail_tickets WHERE refund_detail_id=$1`, detailID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RefundDetailTicket, 0)
	for rows.Next() {
		var record RefundDetailTicket
		if err := rows.Scan(&record.ID, &record.ArrivalStation, &record.CarsNumber, &record.DepartureStation,
			&record.IdentityNumber, &record.IdentityType, &record.Name, &record.OrderNumber, &record.PurchasePrice,
			&record.SeatNumber, &record.TicketClass, &record.TicketNumber, &record.RefundDetailID,
			&record.DepartureDate, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) listRefundWebhooks(ctx context.Context, refundID string) ([]RefundWebhookCall, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, refund_ref, source, payload, response, response_status,
		refund_id, created_at, updated_at FROM refund_webhook_calls WHERE refund_id=$1`, refundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RefundWebhookCall, 0)
	for rows.Next() {
		var record RefundWebhookCall
		if err := rows.Scan(&record.ID, &record.RefundReference, &record.Source, &record.Payload,
			&record.Response, &record.ResponseStatus, &record.RefundID, &record.CreatedAt, &record.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) ListRefundLogs(ctx context.Context, filters []RefundLogFilter) ([]RefundLog, error) {
	query := `SELECT id,type,location,detail,msg,notes,created_at,updated_at,deleted_at FROM refund_logs`
	conditions := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters)*2)
	jakarta, _ := time.LoadLocation("Asia/Jakarta")
	columns := []string{"type", "location", "msg", "notes"}
	for _, filter := range filters {
		if filter.Value == "" {
			continue
		}
		if filter.Column >= 0 && filter.Column < len(columns) {
			args = append(args, "%"+filter.Value+"%")
			conditions = append(conditions, fmt.Sprintf("%s ILIKE $%d", columns[filter.Column], len(args)))
		} else if filter.Column == 4 {
			day, err := time.ParseInLocation("02-01-2006", filter.Value, jakarta)
			if err != nil {
				continue
			}
			args = append(args, day, day.AddDate(0, 0, 1))
			conditions = append(conditions, fmt.Sprintf("created_at >= $%d AND created_at < $%d", len(args)-1, len(args)))
		}
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RefundLog, 0)
	for rows.Next() {
		var record RefundLog
		if err := rows.Scan(&record.ID, &record.Type, &record.Location, &record.Detail, &record.Message,
			&record.Notes, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) ConfigurationInt(ctx context.Context, name string, fallback int) (int, error) {
	var raw string
	err := s.pool.QueryRow(ctx, `SELECT config_value FROM configurations WHERE config_name=$1 LIMIT 1`, name).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback, nil
	}
	return value, nil
}

func (s *Store) ReserveRefundRetry(ctx context.Context, id string, expectedAttempts, maxAttempts int, at time.Time, intent json.RawMessage) (Refund, error) {
	if expectedAttempts >= maxAttempts {
		return Refund{}, ErrRetryLimit
	}
	if err := ValidateSanitizedJSON(intent); err != nil {
		return Refund{}, err
	}
	wrappedIntent, _ := json.Marshal([]json.RawMessage{intent})
	written, err := scanRefund(s.pool.QueryRow(ctx, `UPDATE refunds SET
		retry_attempt=COALESCE(retry_attempt,'[]'::jsonb) || jsonb_build_array($4::text),
		request_data=COALESCE(request_data,'[]'::jsonb) || $5::jsonb,
		refund_status='retry', updated_at=now()
		WHERE id=$1 AND refund_status='fail'
		AND jsonb_array_length(COALESCE(retry_attempt,'[]'::jsonb))=$2 AND $2 < $3
		RETURNING `+refundSelectColumns, id, expectedAttempts, maxAttempts,
		at.In(time.FixedZone("WIB", 7*60*60)).Format("2006-01-02 15:04:05"), wrappedIntent))
	if err == nil {
		return written, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Refund{}, err
	}
	current, getErr := scanRefund(s.pool.QueryRow(ctx, refundSelect+` WHERE id=$1`, id))
	if getErr != nil {
		return Refund{}, getErr
	}
	if current.Status == nil || *current.Status != RefundFail {
		return Refund{}, ErrRetryNotAllowed
	}
	var attempts []string
	_ = json.Unmarshal(current.RetryAttempt, &attempts)
	if len(attempts) >= maxAttempts {
		return Refund{}, ErrRetryLimit
	}
	return Refund{}, ErrRetryConflict
}

func (s *Store) CompleteRefundRetry(ctx context.Context, id string, payoutID *string, response json.RawMessage) error {
	if err := ValidateSanitizedJSON(response); err != nil {
		return err
	}
	command, err := s.pool.Exec(ctx, `UPDATE refunds SET disbursement_id=$2,
		disbursement_response=$3, refund_status='retry', updated_at=now() WHERE id=$1`, id, payoutID, jsonOrNil(response))
	if err == nil && command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) FailRefundRetry(ctx context.Context, id, reason string, response json.RawMessage) error {
	return s.RecordRefundPayoutFailure(ctx, id, reason, response)
}
