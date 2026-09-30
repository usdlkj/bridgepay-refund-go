package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bridgepay-refund-go/internal/backoffice"
	"bridgepay-refund-go/internal/health"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/refundbank"
	refundreport "bridgepay-refund-go/internal/report"
	"bridgepay-refund-go/internal/storage"
)

func TestLivenessAndCorrelationID(t *testing.T) {
	tracker := lifecycle.New()
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), health.New(time.Second), tracker)
	req := httptest.NewRequest(http.MethodGet, "/health/liveness", nil)
	req.Header.Set("X-Request-ID", "synthetic-request")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d", res.Code)
	}
	if got := res.Header().Get("X-Request-ID"); got != "synthetic-request" {
		t.Fatalf("X-Request-ID = %q", got)
	}
}

type stubBanks struct{}

func (stubBanks) PublicList(context.Context) (refundbank.PublicListResponse, error) {
	code := "BCA"
	return refundbank.PublicListResponse{RetCode: 0, RetMsg: "success", RetData: []refundbank.PublicBank{{Code: &code, Name: "Bank Central Asia"}}}, nil
}
func (stubBanks) List(context.Context, []storage.BankFilter) ([]refundbank.Record, error) {
	return []refundbank.Record{}, nil
}
func (stubBanks) View(context.Context, string) (refundbank.Record, error) {
	return refundbank.Record{}, storage.ErrNotFound
}
func (stubBanks) Update(context.Context, string, *storage.BankStatus, **time.Time) (refundbank.Record, error) {
	return refundbank.Record{}, storage.ErrNotFound
}
func (stubBanks) Sync(context.Context) error { return nil }

type stubIluma struct{}

func (stubIluma) CheckAccount(context.Context, iluma.CheckAccountRequest) (iluma.CheckAccountResponse, error) {
	return iluma.CheckAccountResponse{RetCode: 0, Message: "Success", RetData: &iluma.CheckAccountData{Status: "success"}, SignMsg: "signature"}, nil
}
func (stubIluma) Callback(context.Context, json.RawMessage) (map[string]string, error) {
	return map[string]string{"message": "OK"}, nil
}

type stubRefund struct{ createCalls, statusCalls int }

func (s *stubRefund) Create(context.Context, refund.CreateRequest) (refund.Response, error) {
	s.createCalls++
	return refund.Response{RetCode: 0, RetMsg: "Success", RetData: refund.ResponseData{Invoice: refund.CreateInvoice{OrderID: "ORDER-1", Status: "PG process"}}, SignMsg: "signature"}, nil
}

type stubWebhook struct {
	token string
	raw   json.RawMessage
}

func (s *stubWebhook) Accept(_ context.Context, token string, raw json.RawMessage) (map[string]string, error) {
	s.token, s.raw = token, raw
	return map[string]string{"message": "OK"}, nil
}
func (s *stubRefund) Status(context.Context, refund.StatusRequest) (refund.Response, error) {
	s.statusCalls++
	return refund.Response{RetCode: 0, RetMsg: "success", RetData: refund.ResponseData{Invoice: refund.StatusInvoice{OrderID: "ORDER-1"}}, SignMsg: "signature"}, nil
}

func TestStep95RoutesAndServiceAuthentication(t *testing.T) {
	tracker := lifecycle.New()
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), health.New(time.Second), tracker, Dependencies{Banks: stubBanks{}, Iluma: stubIluma{}, ServiceKey: "shared"})

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v2/bankCodes", nil))
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"code":"BCA"`)) {
		t.Fatalf("bankCodes = %d %s", res.Code, res.Body.String())
	}

	valid := []byte(`{"reqData":{"account":{"bankId":"BCA","accountNo":"123","accountType":"saving","idNo":"1","idType":"ktp","name":"A"}},"signMsg":"sig"}`)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/checkAccount", bytes.NewReader(valid)))
	if res.Code != http.StatusCreated || !bytes.Contains(res.Body.Bytes(), []byte(`"signMsg":"signature"`)) {
		t.Fatalf("checkAccount = %d %s", res.Code, res.Body.String())
	}

	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/banks/sync", bytes.NewReader([]byte(`{}`))))
	if res.Code != http.StatusUnauthorized || !bytes.Contains(res.Body.Bytes(), []byte("Missing X-Service-Key header")) {
		t.Fatalf("missing auth = %d %s", res.Code, res.Body.String())
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v2/banks/sync", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("X-Service-Key", "shared")
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusCreated || !bytes.Contains(res.Body.Bytes(), []byte(`"message":"Success"`)) {
		t.Fatalf("sync = %d %s", res.Code, res.Body.String())
	}

	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/checkAccount", bytes.NewReader([]byte(`{}`))))
	if res.Code != http.StatusBadRequest || !bytes.Contains(res.Body.Bytes(), []byte("accountNo should not be empty")) {
		t.Fatalf("validation = %d %s", res.Code, res.Body.String())
	}
}

func TestStep96RefundRoutesValidateAndDispatch(t *testing.T) {
	tracker := lifecycle.New()
	refunds := &stubRefund{}
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), health.New(time.Second), tracker, Dependencies{Refund: refunds})
	create := `{"reqData":{"account":{"bankId":"BCA","accountNo":"123","name":"Test","accountType":"saving","idNo":"1","idType":"1"},"invoice":{"orderId":"ORDER-1","refundAmount":1000,"reason":"reason","passengers":"passenger","originalOrderNumber":"ORDER-0","notifyUrl":"https://example.test","ticketOffice":"kcic"}},"signMsg":"signature"}`
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/transfer", bytes.NewBufferString(create)))
	if res.Code != http.StatusCreated || refunds.createCalls != 1 || !bytes.Contains(res.Body.Bytes(), []byte(`"status":"PG process"`)) {
		t.Fatalf("create=%d %s calls=%d", res.Code, res.Body.String(), refunds.createCalls)
	}
	status := `{"reqData":{"invoice":{"orderId":"ORDER-1"}},"signMsg":"signature"}`
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/transferQuery", bytes.NewBufferString(status)))
	if res.Code != http.StatusCreated || refunds.statusCalls != 1 {
		t.Fatalf("status=%d %s calls=%d", res.Code, res.Body.String(), refunds.statusCalls)
	}
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v2/transfer", bytes.NewBufferString(`{"reqData":{},"signMsg":"signature"}`)))
	if res.Code != http.StatusBadRequest || refunds.createCalls != 1 {
		t.Fatalf("invalid=%d %s calls=%d", res.Code, res.Body.String(), refunds.createCalls)
	}
}

func TestStep97XenditWebhookPersistsBeforeAcknowledgement(t *testing.T) {
	tracker := lifecycle.New()
	webhook := &stubWebhook{}
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), health.New(time.Second), tracker, Dependencies{Webhook: webhook})
	request := httptest.NewRequest(http.MethodPost, "/api/v2/webhook/xendit/disbursement", bytes.NewReader([]byte(`{"event":"payout.succeeded"}`)))
	request.Header.Set("X-Callback-Token", "callback-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || webhook.token != "callback-secret" || string(webhook.raw) != `{"event":"payout.succeeded"}` {
		t.Fatalf("status=%d token=%q raw=%s body=%s", response.Code, webhook.token, webhook.raw, response.Body.String())
	}
}

type stubBackoffice struct{}

func (stubBackoffice) List(context.Context, []storage.RefundFilter) ([]backoffice.Refund, error) {
	return []backoffice.Refund{{ID: "refund-1"}}, nil
}
func (stubBackoffice) View(context.Context, string, bool) (*backoffice.Refund, error) {
	return &backoffice.Refund{ID: "refund-1"}, nil
}
func (stubBackoffice) Logs(context.Context, []storage.RefundLogFilter) ([]backoffice.RefundLog, error) {
	return []backoffice.RefundLog{}, nil
}
func (stubBackoffice) Retry(context.Context, string) error { return nil }

type stubReports struct{}

func (stubReports) List(context.Context, []storage.ReportFilter) ([]refundreport.Record, error) {
	return []refundreport.Record{{ID: "report-1"}}, nil
}
func (stubReports) Create(context.Context, refundreport.CreateRequest) (refundreport.Record, error) {
	return refundreport.Record{ID: "report-1"}, nil
}
func (stubReports) Download(context.Context, string) (string, storage.ReportType, []byte, error) {
	return "refund01012026", storage.ReportRefund, []byte("PK synthetic"), nil
}

func TestStep98BackofficeAndReportHTTPContracts(t *testing.T) {
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), health.New(time.Second), lifecycle.New(), Dependencies{
		Backoffice: stubBackoffice{}, Reports: stubReports{}, ServiceKey: "shared",
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v2/refunds", bytes.NewBufferString(`{"query":[]}`))
	req.Header.Set("X-Service-Key", "shared")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusCreated || !bytes.Contains(res.Body.Bytes(), []byte(`"id":"refund-1"`)) {
		t.Fatalf("refund list = %d %s", res.Code, res.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v2/report/create", bytes.NewBufferString(`{"date":"2026-01-01","type":"refund"}`))
	req.Header.Set("X-Service-Key", "shared")
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusCreated || !bytes.Contains(res.Body.Bytes(), []byte(`"id":"report-1"`)) {
		t.Fatalf("report create = %d %s", res.Code, res.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v2/report/download/report-1", nil)
	req.Header.Set("X-Service-Key", "shared")
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK || res.Header().Get("Content-Disposition") != "attachment; filename=refund01012026.xlsx" || !bytes.HasPrefix(res.Body.Bytes(), []byte("PK")) {
		t.Fatalf("report download = %d %q %q", res.Code, res.Header().Get("Content-Disposition"), res.Body.String())
	}
}

func TestReadinessFailsClosedAndDrainRejectsWork(t *testing.T) {
	tracker := lifecycle.New()
	checks := health.New(time.Second, health.Check{Name: "dependency", Run: func(context.Context) error {
		return context.DeadlineExceeded
	}})
	handler := New(slog.New(slog.NewTextHandler(io.Discard, nil)), checks, tracker)

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/health/readiness", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d", res.Code)
	}

	tracker.BeginDrain()
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/not-operational", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining status = %d", res.Code)
	}
}
