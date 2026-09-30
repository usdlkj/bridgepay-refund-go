package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

type BankService interface {
	PublicList(context.Context) (refundbank.PublicListResponse, error)
	List(context.Context, []storage.BankFilter) ([]refundbank.Record, error)
	View(context.Context, string) (refundbank.Record, error)
	Update(context.Context, string, *storage.BankStatus, **time.Time) (refundbank.Record, error)
	Sync(context.Context) error
}

type IlumaService interface {
	CheckAccount(context.Context, iluma.CheckAccountRequest) (iluma.CheckAccountResponse, error)
	Callback(context.Context, json.RawMessage) (map[string]string, error)
}

type Dependencies struct {
	Banks   BankService
	Iluma   IlumaService
	Webhook interface {
		Accept(context.Context, string, json.RawMessage) (map[string]string, error)
	}
	Refund interface {
		Create(context.Context, refund.CreateRequest) (refund.Response, error)
		Status(context.Context, refund.StatusRequest) (refund.Response, error)
	}
	Backoffice interface {
		List(context.Context, []storage.RefundFilter) ([]backoffice.Refund, error)
		View(context.Context, string, bool) (*backoffice.Refund, error)
		Logs(context.Context, []storage.RefundLogFilter) ([]backoffice.RefundLog, error)
		Retry(context.Context, string) error
	}
	Reports interface {
		List(context.Context, []storage.ReportFilter) ([]refundreport.Record, error)
		Create(context.Context, refundreport.CreateRequest) (refundreport.Record, error)
		Download(context.Context, string) (string, storage.ReportType, []byte, error)
	}
	ServiceKey string
}

type API struct {
	logger  *slog.Logger
	health  *health.Service
	tracker *lifecycle.Tracker
	deps    Dependencies
}

func New(logger *slog.Logger, healthService *health.Service, tracker *lifecycle.Tracker, optional ...Dependencies) http.Handler {
	var deps Dependencies
	if len(optional) > 0 {
		deps = optional[0]
	}
	api := &API{logger: logger, health: healthService, tracker: tracker, deps: deps}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/liveness", api.liveness)
	mux.HandleFunc("GET /health/readiness", api.readiness)
	mux.HandleFunc("GET /health", api.readiness)
	mux.HandleFunc("GET /api/v2/bankCodes", api.bankCodes)
	mux.HandleFunc("POST /api/v2/checkAccount", api.checkAccount)
	mux.HandleFunc("POST /api/v2/webhook/iluma/bank-validator", api.ilumaCallback)
	mux.HandleFunc("POST /api/v2/webhook/xendit/disbursement", api.xenditCallback)
	mux.HandleFunc("POST /api/v2/transfer", api.createRefund)
	mux.HandleFunc("POST /api/v2/transferQuery", api.refundStatus)
	mux.Handle("POST /api/v2/banks/sync", api.serviceAuth(http.HandlerFunc(api.bankSync)))
	mux.Handle("POST /api/v2/banks", api.serviceAuth(http.HandlerFunc(api.bankList)))
	mux.Handle("POST /api/v2/banks/", api.serviceAuth(http.HandlerFunc(api.bankList)))
	mux.Handle("GET /api/v2/banks/{id}", api.serviceAuth(http.HandlerFunc(api.bankView)))
	mux.Handle("POST /api/v2/banks/{id}", api.serviceAuth(http.HandlerFunc(api.bankUpdate)))
	mux.Handle("POST /api/v2/refunds/banks/{id}", api.serviceAuth(http.HandlerFunc(api.bankUpdate)))
	mux.Handle("POST /api/v2/refunds", api.serviceAuth(http.HandlerFunc(api.refundList)))
	mux.Handle("POST /api/v2/refunds/", api.serviceAuth(http.HandlerFunc(api.refundList)))
	mux.Handle("POST /api/v2/refunds/log", api.serviceAuth(http.HandlerFunc(api.refundLog)))
	mux.Handle("GET /api/v2/refunds/refundDetail/{id}", api.serviceAuth(http.HandlerFunc(api.refundDetail)))
	mux.Handle("POST /api/v2/refunds/retry/{id}", api.serviceAuth(http.HandlerFunc(api.refundRetry)))
	mux.Handle("GET /api/v2/refunds/{id}", api.serviceAuth(http.HandlerFunc(api.refundView)))
	mux.Handle("POST /api/v2/report", api.serviceAuth(http.HandlerFunc(api.reportList)))
	mux.Handle("POST /api/v2/report/", api.serviceAuth(http.HandlerFunc(api.reportList)))
	mux.Handle("POST /api/v2/report/create", api.serviceAuth(http.HandlerFunc(api.reportCreate)))
	mux.Handle("GET /api/v2/report/download/{id}", api.serviceAuth(http.HandlerFunc(api.reportDownload)))
	return api.correlation(api.logging(api.admission(mux)))
}

type listRequest struct {
	Query []struct {
		Data   int `json:"data"`
		Search struct {
			Value string `json:"value"`
		} `json:"search"`
	} `json:"query"`
}

func (a *API) refundList(w http.ResponseWriter, r *http.Request) {
	if a.deps.Backoffice == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund Backoffice service unavailable"})
		return
	}
	var request listRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	filters := make([]storage.RefundFilter, 0, len(request.Query))
	for _, item := range request.Query {
		if item.Data < 0 || item.Data > 5 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"query.data must not be greater than 5", "query.data must not be less than 0"}, "error": "Bad Request", "statusCode": 400})
			return
		}
		filters = append(filters, storage.RefundFilter{Column: item.Data, Value: item.Search.Value})
	}
	result, err := a.deps.Backoffice.List(r.Context(), filters)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) refundLog(w http.ResponseWriter, r *http.Request) {
	if a.deps.Backoffice == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund Backoffice service unavailable"})
		return
	}
	var request listRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	filters := make([]storage.RefundLogFilter, 0, len(request.Query))
	for _, item := range request.Query {
		if item.Data < 0 || item.Data > 4 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"query.data must not be greater than 4", "query.data must not be less than 0"}, "error": "Bad Request", "statusCode": 400})
			return
		}
		filters = append(filters, storage.RefundLogFilter{Column: item.Data, Value: item.Search.Value})
	}
	result, err := a.deps.Backoffice.Logs(r.Context(), filters)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) refundView(w http.ResponseWriter, r *http.Request) {
	a.refundAdminView(w, r, false)
}

func (a *API) refundDetail(w http.ResponseWriter, r *http.Request) {
	a.refundAdminView(w, r, true)
}

func (a *API) refundAdminView(w http.ResponseWriter, r *http.Request, relationDetail bool) {
	if a.deps.Backoffice == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund Backoffice service unavailable"})
		return
	}
	result, err := a.deps.Backoffice.View(r.Context(), r.PathValue("id"), relationDetail)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) refundRetry(w http.ResponseWriter, r *http.Request) {
	if a.deps.Backoffice == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund Backoffice service unavailable"})
		return
	}
	if err := a.deps.Backoffice.Retry(r.Context(), r.PathValue("id")); err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"status": 200, "message": "Success"})
}

func (a *API) reportList(w http.ResponseWriter, r *http.Request) {
	if a.deps.Reports == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Report service unavailable"})
		return
	}
	var request listRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	filters := make([]storage.ReportFilter, 0, len(request.Query))
	for _, item := range request.Query {
		if item.Data < 0 || item.Data > 5 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"query.data must not be greater than 5", "query.data must not be less than 0"}, "error": "Bad Request", "statusCode": 400})
			return
		}
		filters = append(filters, storage.ReportFilter{Column: item.Data, Value: item.Search.Value})
	}
	result, err := a.deps.Reports.List(r.Context(), filters)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) reportCreate(w http.ResponseWriter, r *http.Request) {
	if a.deps.Reports == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Report service unavailable"})
		return
	}
	var request refundreport.CreateRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	result, err := a.deps.Reports.Create(r.Context(), request)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{err.Error()}, "error": "Bad Request", "statusCode": 400})
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) reportDownload(w http.ResponseWriter, r *http.Request) {
	if a.deps.Reports == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Report service unavailable"})
		return
	}
	name, _, contents, err := a.deps.Reports.Download(r.Context(), r.PathValue("id"))
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", "attachment; filename="+name+".xlsx")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(contents)
}

func (a *API) xenditCallback(w http.ResponseWriter, r *http.Request) {
	if a.deps.Webhook == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund webhook service unavailable"})
		return
	}
	var raw json.RawMessage
	if !decodeJSON(w, r, &raw) {
		return
	}
	result, err := a.deps.Webhook.Accept(r.Context(), r.Header.Get("X-Callback-Token"), raw)
	if err != nil {
		var webhookErr *refund.WebhookError
		if errors.As(err, &webhookErr) {
			writeJSON(w, webhookErr.HTTPStatus, webhookErr.Payload)
			return
		}
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) createRefund(w http.ResponseWriter, r *http.Request) {
	if a.deps.Refund == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund service unavailable"})
		return
	}
	var request refund.CreateRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if problems := refund.ValidateCreate(request); len(problems) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": problems, "error": "Bad Request", "statusCode": 400})
		return
	}
	result, err := a.deps.Refund.Create(r.Context(), request)
	if err != nil {
		a.refundError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) refundStatus(w http.ResponseWriter, r *http.Request) {
	if a.deps.Refund == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Refund service unavailable"})
		return
	}
	var request refund.StatusRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if problems := refund.ValidateStatus(request); len(problems) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": problems, "error": "Bad Request", "statusCode": 400})
		return
	}
	result, err := a.deps.Refund.Status(r.Context(), request)
	if err != nil {
		a.refundError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) refundError(w http.ResponseWriter, r *http.Request, err error) {
	a.logger.ErrorContext(r.Context(), "refund request failed", "request_id", RequestID(r), "error", err)
	var public *refund.PublicError
	if errors.As(err, &public) {
		writeJSON(w, public.HTTPStatus, public.Payload)
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"retCode": -1, "retMsg": "Failed to process refund request"})
}

func (a *API) bankCodes(w http.ResponseWriter, r *http.Request) {
	if a.deps.Banks == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Bank service unavailable"})
		return
	}
	result, err := a.deps.Banks.PublicList(r.Context())
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) checkAccount(w http.ResponseWriter, r *http.Request) {
	if a.deps.Iluma == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Iluma service unavailable"})
		return
	}
	var request iluma.CheckAccountRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if problems := iluma.ValidateCheckAccount(request); len(problems) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": problems, "error": "Bad Request", "statusCode": 400})
		return
	}
	result, err := a.deps.Iluma.CheckAccount(r.Context(), request)
	if err != nil {
		a.logger.ErrorContext(r.Context(), "Iluma account check failed", "request_id", RequestID(r), "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"retCode": -1, "retMsg": "Failed to check account"})
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) ilumaCallback(w http.ResponseWriter, r *http.Request) {
	if a.deps.Iluma == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Iluma service unavailable"})
		return
	}
	var raw json.RawMessage
	if !decodeJSON(w, r, &raw) {
		return
	}
	result, _ := a.deps.Iluma.Callback(r.Context(), raw)
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) bankSync(w http.ResponseWriter, r *http.Request) {
	if a.deps.Banks == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Bank service unavailable"})
		return
	}
	if err := a.deps.Banks.Sync(r.Context()); err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"status": 200, "message": "Success"})
}

type bankListRequest struct {
	Query []struct {
		Data   int `json:"data"`
		Search struct {
			Value string `json:"value"`
		} `json:"search"`
	} `json:"query"`
}

func (a *API) bankList(w http.ResponseWriter, r *http.Request) {
	if a.deps.Banks == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "Bank service unavailable"})
		return
	}
	var request bankListRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	filters := make([]storage.BankFilter, 0, len(request.Query))
	for _, column := range request.Query {
		if column.Data >= 0 && column.Data <= 2 {
			filters = append(filters, storage.BankFilter{Column: column.Data, Value: column.Search.Value})
		}
	}
	result, err := a.deps.Banks.List(r.Context(), filters)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) bankView(w http.ResponseWriter, r *http.Request) {
	result, err := a.deps.Banks.View(r.Context(), r.PathValue("id"))
	if errors.Is(err, storage.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"status": 404, "message": "Refund bank not found"})
		return
	}
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type bankUpdateRequest struct {
	BankStatus *storage.BankStatus `json:"bankStatus"`
	DeletedAt  json.RawMessage     `json:"deletedAt"`
}

func (a *API) bankUpdate(w http.ResponseWriter, r *http.Request) {
	var request bankUpdateRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.BankStatus != nil && *request.BankStatus != storage.BankEnabled && *request.BankStatus != storage.BankDisabled {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"bankStatus must be one of the following values: enable, disable"}, "error": "Bad Request", "statusCode": 400})
		return
	}
	var deletedAt **time.Time
	if len(request.DeletedAt) > 0 {
		var parsed *time.Time
		if string(request.DeletedAt) != "null" && string(request.DeletedAt) != `""` {
			var raw string
			if json.Unmarshal(request.DeletedAt, &raw) != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"deletedAt must be a valid ISO 8601 date string"}, "error": "Bad Request", "statusCode": 400})
				return
			}
			value, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"message": []string{"deletedAt must be a valid ISO 8601 date string"}, "error": "Bad Request", "statusCode": 400})
				return
			}
			parsed = &value
		}
		deletedAt = &parsed
	}
	result, err := a.deps.Banks.Update(r.Context(), r.PathValue("id"), request.BankStatus, deletedAt)
	if errors.Is(err, storage.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"status": 404, "message": "Refund bank not found"})
		return
	}
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) serviceAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.deps.ServiceKey == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Service-to-service authentication not configured", "error": "Unauthorized", "statusCode": 401})
			return
		}
		provided := r.Header.Get("X-Service-Key")
		if provided == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Missing X-Service-Key header", "error": "Unauthorized", "statusCode": 401})
			return
		}
		if len(provided) != len(a.deps.ServiceKey) || subtle.ConstantTimeCompare([]byte(provided), []byte(a.deps.ServiceKey)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Invalid service key", "error": "Unauthorized", "statusCode": 401})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "Invalid JSON body", "error": "Bad Request", "statusCode": 400})
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "Invalid JSON body", "error": "Bad Request", "statusCode": 400})
		return false
	}
	return true
}

func (a *API) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.logger.ErrorContext(r.Context(), "request failed", "request_id", RequestID(r), "error", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"status": 500, "message": err.Error()})
}

func (a *API) liveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) readiness(w http.ResponseWriter, r *http.Request) {
	if !a.tracker.Snapshot().Accepting {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	report := a.health.Readiness(r.Context())
	status := http.StatusOK
	if report.Status != "ok" {
		status = http.StatusServiceUnavailable
		a.logger.WarnContext(r.Context(), "readiness check failed", "dependencies", report.FailedNames())
	}
	writeJSON(w, status, report)
}

func (a *API) admission(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/liveness" {
			next.ServeHTTP(w, r)
			return
		}
		done, ok := a.tracker.TryBegin("http", false)
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		defer done()
		next.ServeHTTP(w, r)
	})
}

type contextKey string

const requestIDKey contextKey = "request-id"

func (a *API) correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = randomID()
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), requestID)))
	})
}

func (a *API) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		a.logger.InfoContext(r.Context(), "http request",
			"request_id", RequestID(r),
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(b[:])
}
