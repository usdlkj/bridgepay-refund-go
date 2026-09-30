package report

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"bridgepay-refund-go/internal/storage"
)

type reportStore struct {
	report storage.Report
	rows   []storage.RefundAdminRecord
	saved  json.RawMessage
}

func (*reportStore) ListReports(context.Context, []storage.ReportFilter) ([]storage.Report, error) {
	return nil, nil
}
func (s *reportStore) CreateReport(context.Context, storage.ReportType, string, time.Time, time.Time) (storage.Report, error) {
	return s.report, nil
}
func (*reportStore) ScheduledReportExists(context.Context, storage.ReportType, time.Time) (bool, error) {
	return false, nil
}
func (s *reportStore) GetReport(context.Context, string) (storage.Report, error) {
	r := s.report
	r.Data = s.saved
	return r, nil
}
func (*reportStore) PendingReports(context.Context) ([]storage.Report, error) { return nil, nil }
func (s *reportStore) CompleteReport(_ context.Context, _ string, data json.RawMessage) error {
	s.saved = data
	return nil
}
func (s *reportStore) ListRefundsForReport(context.Context, time.Time, time.Time) ([]storage.RefundAdminRecord, error) {
	return s.rows, nil
}
func (*reportStore) ListIlumaLogsForReport(context.Context, time.Time, time.Time) ([]storage.IlumaCallLog, error) {
	return nil, nil
}
func (_ *reportStore) WithAdvisoryLock(ctx context.Context, _ string, fn func(context.Context) error) (bool, error) {
	return true, fn(ctx)
}

func TestRefundReportPersistsNodeFieldsAndDownloadsXLSX(t *testing.T) {
	zone, _ := time.LoadLocation("Asia/Jakarta")
	day := time.Date(2026, 9, 20, 0, 0, 0, 0, zone)
	name := "refund20092026"
	ga := "GA-1"
	amount := "10000"
	store := &reportStore{report: storage.Report{ID: "report-1", Name: &name, RefundStart: &day,
		Type: storage.ReportRefund, Status: storage.ReportProcessing}, rows: []storage.RefundAdminRecord{{Refund: storage.Refund{
		RefundGANumber: &ga, Amount: &amount, CreatedAt: day.Add(8 * time.Hour),
		AmountData: json.RawMessage(`{"fee":2100,"tax":231,"totalAmount":12331}`),
		BankData:   json.RawMessage(`{"bankCode":"ID_BCA","bankNumber":"******7890","bankName":"Customer"}`),
	}}}}
	service := New(store, func(_ string, _ func(context.Context) error) bool { return true })
	if err := service.Generate(context.Background(), store.report); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(store.saved) || !contains(store.saved, `"refundTrandeNo":"GA-1"`) ||
		!contains(store.saved, `"refundAccount":"******7890"`) || !contains(store.saved, `"refundCharge":2100`) {
		t.Fatalf("saved report = %s", store.saved)
	}
	title, kind, contents, err := service.Download(context.Background(), "report-1")
	if err != nil {
		t.Fatal(err)
	}
	if title != name || kind != storage.ReportRefund || len(contents) < 4 || string(contents[:2]) != "PK" {
		t.Fatalf("title=%q kind=%q bytes=%d", title, kind, len(contents))
	}
}

func TestNextRunUsesJakartaSchedule(t *testing.T) {
	zone, _ := time.LoadLocation("Asia/Jakarta")
	now := time.Date(2026, 9, 20, 4, 0, 0, 0, zone)
	if got := nextRun(now, 4); !got.Equal(time.Date(2026, 9, 21, 4, 0, 0, 0, zone)) {
		t.Fatalf("next = %s", got)
	}
}

func contains(value json.RawMessage, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if string(value[i:i+len(part)]) == part {
			return true
		}
	}
	return false
}
