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

func (s *Store) ListReports(ctx context.Context, filters []ReportFilter) ([]Report, error) {
	query := reportSelect
	conditions := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters)*2)
	jakarta, _ := time.LoadLocation("Asia/Jakarta")
	for _, filter := range filters {
		if filter.Column != 3 {
			return nil, fmt.Errorf("report filter column %d does not exist in the frozen Node schema", filter.Column)
		}
		day, err := time.ParseInLocation("02-01-2006", filter.Value, jakarta)
		if err != nil {
			continue
		}
		args = append(args, day, day.AddDate(0, 0, 1))
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d AND created_at < $%d", len(args)-1, len(args)))
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Report, 0)
	for rows.Next() {
		record, err := scanReport(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) CreateReport(ctx context.Context, reportType ReportType, name string, start, end time.Time) (Report, error) {
	id := NewID()
	return scanReport(s.pool.QueryRow(ctx, `INSERT INTO reports
		(id,name,refund_start_date,refund_end_date,report_type,report_status)
		VALUES($1,$2,$3::date,$4::date,$5,'process') RETURNING `+reportSelectColumns,
		id, name, start, end, reportType))
}

func (s *Store) ScheduledReportExists(ctx context.Context, reportType ReportType, start time.Time) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM reports
		WHERE report_type=$1 AND refund_start_date=$2::date AND deleted_at IS NULL)`, reportType, start).Scan(&exists)
	return exists, err
}

func (s *Store) GetReport(ctx context.Context, id string) (Report, error) {
	return scanReport(s.pool.QueryRow(ctx, reportSelect+` WHERE id=$1`, id))
}

func (s *Store) PendingReports(ctx context.Context) ([]Report, error) {
	rows, err := s.pool.Query(ctx, reportSelect+` WHERE report_status='process'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Report, 0)
	for rows.Next() {
		record, err := scanReport(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) CompleteReport(ctx context.Context, id string, data json.RawMessage) error {
	command, err := s.pool.Exec(ctx, `UPDATE reports SET report_status='completed',report_data=$2,updated_at=now() WHERE id=$1`, id, data)
	if err == nil && command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) ListRefundsForReport(ctx context.Context, start, end time.Time) ([]RefundAdminRecord, error) {
	rows, err := s.pool.Query(ctx, refundSelect+` WHERE refund_status IN ('success','done') AND created_at >= $1 AND created_at <= $2`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RefundAdminRecord, 0)
	for rows.Next() {
		record, err := scanRefund(rows)
		if err != nil {
			return nil, err
		}
		admin := RefundAdminRecord{Refund: record}
		if record.RefundDetailID != nil {
			detail, err := scanRefundDetail(s.pool.QueryRow(ctx, refundDetailSelect+` WHERE id=$1`, *record.RefundDetailID))
			if err != nil && !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			if err == nil {
				admin.Detail = &detail
			}
		}
		result = append(result, admin)
	}
	return result, rows.Err()
}

func (s *Store) ListIlumaLogsForReport(ctx context.Context, start, end time.Time) ([]IlumaCallLog, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,url,method,func,payload,request,response,
		created_at,updated_at,deleted_at FROM iluma_call_logs WHERE created_at >= $1 AND created_at <= $2`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]IlumaCallLog, 0)
	for rows.Next() {
		var record IlumaCallLog
		if err := rows.Scan(&record.ID, &record.URL, &record.Method, &record.Function, &record.Payload,
			&record.Request, &record.Response, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) WithAdvisoryLock(ctx context.Context, name string, fn func(context.Context) error) (bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, name).Scan(&locked); err != nil || !locked {
		return locked, err
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, name) }()
	return true, fn(ctx)
}

const reportSelectColumns = `id,name,refund_start_date,refund_end_date,report_data,report_type,report_status,created_at,updated_at,deleted_at`
const reportSelect = `SELECT ` + reportSelectColumns + ` FROM reports`

func scanReport(row rowScanner) (Report, error) {
	var record Report
	err := row.Scan(&record.ID, &record.Name, &record.RefundStart, &record.RefundEnd, &record.Data,
		&record.Type, &record.Status, &record.CreatedAt, &record.UpdatedAt, &record.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Report{}, ErrNotFound
	}
	return record, err
}
