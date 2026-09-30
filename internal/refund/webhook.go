package refund

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"bridgepay-refund-go/internal/storage"
)

const xenditCallbackEvent = "refund.xendit.callback.process"

type XenditWebhook struct {
	Event      string            `json:"event"`
	BusinessID string            `json:"business_id"`
	Created    string            `json:"created"`
	Data       XenditWebhookData `json:"data"`
}

type XenditWebhookData struct {
	ID                string                   `json:"id"`
	Amount            *float64                 `json:"amount"`
	ChannelCode       string                   `json:"channel_code"`
	Currency          string                   `json:"currency"`
	Status            string                   `json:"status"`
	Description       string                   `json:"description,omitempty"`
	ReferenceID       string                   `json:"reference_id"`
	Created           string                   `json:"created"`
	Updated           string                   `json:"updated"`
	FailureCode       string                   `json:"failure_code,omitempty"`
	ChannelProperties *XenditChannelProperties `json:"channel_properties"`
}

type XenditChannelProperties struct {
	AccountNumber     string `json:"account_number"`
	AccountHolderName string `json:"account_holder_name,omitempty"`
	AccountType       string `json:"account_type,omitempty"`
}

type WebhookError struct {
	HTTPStatus int
	Payload    any
	Cause      error
}

func (e *WebhookError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "refund webhook failed"
}

type WebhookStore interface {
	InsertRefundWebhookCall(context.Context, storage.RefundWebhookInsert) (string, error)
	PrepareRefundWebhook(context.Context, string, string, string, string, json.RawMessage, int, time.Duration) (storage.RefundWebhookWork, error)
	ReleaseRefundWebhook(context.Context, string) error
	CompleteRefundWebhook(context.Context, string, string, json.RawMessage, int) error
	FailRefundWebhook(context.Context, string, string, string, int) error
	InsertTicketingCallLog(context.Context, storage.TicketingCallLog) error
	ConfigurationValue(context.Context, string) (string, error)
}

type WebhookPublisher interface {
	PublishEvent(context.Context, string, any, string) error
}

type BalanceProvider interface {
	Balance(context.Context, string) ProviderResult
}

type TicketingNotifier interface {
	Notify(context.Context, string, any) TicketingResult
}

type WebhookService struct {
	store       WebhookStore
	credentials CredentialProvider
	provider    BalanceProvider
	notifier    TicketingNotifier
	signer      Signer
	publisher   WebhookPublisher
	environment string
	logger      *slog.Logger
}

func NewWebhookService(store WebhookStore, credentials CredentialProvider, provider BalanceProvider, notifier TicketingNotifier, signer Signer, publisher WebhookPublisher, environment string, logger *slog.Logger) *WebhookService {
	if logger == nil {
		logger = slog.Default()
	}
	return &WebhookService{store: store, credentials: credentials, provider: provider, notifier: notifier,
		signer: signer, publisher: publisher, environment: environment, logger: logger}
}

func ValidateXenditWebhook(payload XenditWebhook) []string {
	var problems []string
	for _, field := range []struct{ name, value string }{
		{"event", payload.Event}, {"business_id", payload.BusinessID}, {"created", payload.Created},
		{"data.id", payload.Data.ID}, {"data.channel_code", payload.Data.ChannelCode},
		{"data.currency", payload.Data.Currency}, {"data.status", payload.Data.Status},
		{"data.reference_id", payload.Data.ReferenceID}, {"data.created", payload.Data.Created},
		{"data.updated", payload.Data.Updated},
	} {
		if strings.TrimSpace(field.value) == "" {
			problems = append(problems, field.name+" should not be empty")
		}
	}
	if payload.Data.Amount == nil {
		problems = append(problems, "data.amount must be a number conforming to the specified constraints")
	}
	if payload.Data.ChannelProperties == nil || strings.TrimSpace(payload.Data.ChannelProperties.AccountNumber) == "" {
		problems = append(problems, "data.channel_properties.account_number should not be empty")
	}
	return problems
}

// Accept authenticates and durably queues the callback. Balance lookup and
// Ticketing delivery are deliberately left to the background event.
func (s *WebhookService) Accept(ctx context.Context, token string, raw json.RawMessage) (map[string]string, error) {
	if token == "" {
		return nil, webhookError(401, "Callback token is required", nil)
	}
	credential, err := s.credentials.XenditCredential(ctx, s.environment)
	if err != nil {
		return nil, webhookError(500, "Failed to retrieve credential from bridgepay-core", err)
	}
	if credential.CallbackToken == "" {
		return nil, webhookError(500, "Invalid credential: callback token not found", nil)
	}
	if len(token) != len(credential.CallbackToken) || subtle.ConstantTimeCompare([]byte(token), []byte(credential.CallbackToken)) != 1 {
		return nil, webhookError(401, "Invalid callback token", nil)
	}
	var callback XenditWebhook
	if err := json.Unmarshal(raw, &callback); err != nil {
		return nil, badWebhookRequest([]string{"invalid request body"}, err)
	}
	if problems := ValidateXenditWebhook(callback); len(problems) > 0 {
		return nil, badWebhookRequest(problems, nil)
	}
	sanitized := sanitizeProviderJSON(raw)
	dedupeKey := callbackDedupeKey(callback)
	processID, err := s.store.InsertRefundWebhookCall(ctx, storage.RefundWebhookInsert{
		RefundReference: callback.Data.ReferenceID, Source: "xendit", DedupeKey: dedupeKey, Payload: sanitized,
	})
	if err != nil {
		return nil, webhookError(500, "failed handle webhook", err)
	}
	if err := s.publisher.PublishEvent(ctx, xenditCallbackEvent, map[string]string{"id": processID}, "refund.xendit.callback:"+processID); err != nil {
		return nil, webhookError(500, "failed handle webhook", err)
	}
	return map[string]string{"message": "OK"}, nil
}

func (s *WebhookService) Process(ctx context.Context, callbackID string, retryCount, maxRetries int) error {
	work, err := s.prepare(ctx, callbackID)
	if err != nil {
		return err
	}
	if work.State != storage.WebhookFollowUp || work.Refund == nil {
		if work.State == storage.WebhookProcessing {
			return errors.New("refund webhook is already processing")
		}
		return nil
	}
	credential, err := s.credentials.XenditCredential(ctx, s.environment)
	if err != nil {
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, err)
	}
	balanceResult := s.provider.Balance(ctx, credential.SecretKey)
	if balanceResult.Err != nil || balanceResult.Status < 200 || balanceResult.Status >= 300 {
		err := balanceResult.Err
		if err == nil {
			err = fmt.Errorf("Xendit balance HTTP %d", balanceResult.Status)
		}
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, err)
	}
	var balanceEnvelope struct {
		Balance any `json:"balance"`
	}
	if err := json.Unmarshal(balanceResult.Data, &balanceEnvelope); err != nil {
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, errors.New("invalid Xendit balance response"))
	}
	payload, err := s.ticketingPayload(*work.Refund, work.Call.Payload, work.Outcome, balanceEnvelope.Balance)
	if err != nil {
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, err)
	}
	result := s.notifier.Notify(ctx, notifyURL(work.Refund.Data), payload)
	payloadJSON, _ := json.Marshal(payload)
	responseJSON := sanitizeProviderJSON(result.safeJSON())
	if logErr := s.store.InsertTicketingCallLog(ctx, storage.TicketingCallLog{
		RefundNumber: value(work.Refund.RefundGANumber), Payload: payloadJSON, Response: responseJSON,
	}); logErr != nil {
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, logErr)
	}
	if result.Err != nil || !result.Accepted() {
		err := result.Err
		if err == nil {
			err = fmt.Errorf("Ticketing rejected refund notification with HTTP %d", result.Status)
		}
		return s.followUpFailure(ctx, callbackID, work.Outcome, retryCount, maxRetries, err)
	}
	notification, _ := json.Marshal(map[string]any{
		"payload": payload, "initAt": result.StartedAt.Format("2006-01-02 15:04:05"),
		"responseData": result.Body, "responseAt": result.FinishedAt.Format("2006-01-02 15:04:05"),
	})
	return s.store.CompleteRefundWebhook(ctx, callbackID, work.Outcome, notification, result.Status)
}

func (s *WebhookService) prepare(ctx context.Context, callbackID string) (storage.RefundWebhookWork, error) {
	tryLimit := s.configurationInt(ctx, "REFUND_TRY_COUNT", 1)
	tryMinutes := s.configurationInt(ctx, "\u2060REFUND_TRY_TIME_PERIOD", 10)
	if tryMinutes < 10 {
		tryMinutes = 60
	}
	// PrepareRefundWebhook parses the persisted call under a row lock; blank
	// arguments ask it to use the payload fields stored with that call.
	return s.store.PrepareRefundWebhook(ctx, callbackID, "", "", "", nil, tryLimit, time.Duration(tryMinutes)*time.Minute)
}

func (s *WebhookService) configurationInt(ctx context.Context, name string, fallback int) int {
	raw, err := s.store.ConfigurationValue(ctx, name)
	if err != nil {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func (s *WebhookService) followUpFailure(ctx context.Context, id, outcome string, retryCount, maxRetries int, cause error) error {
	s.logger.ErrorContext(ctx, "refund callback follow-up failed", "callback_id", id, "error", cause)
	var persistErr error
	if retryCount >= maxRetries {
		persistErr = s.store.FailRefundWebhook(ctx, id, outcome, cause.Error(), 500)
	} else {
		persistErr = s.store.ReleaseRefundWebhook(ctx, id)
	}
	if persistErr != nil {
		return errors.Join(cause, persistErr)
	}
	return cause
}

type successNotification struct {
	Balance      any    `json:"balance"`
	BankCode     string `json:"bankCode"`
	BankNo       string `json:"bankNo"`
	CurType      string `json:"curType"`
	Fee          any    `json:"fee"`
	MWNo         string `json:"mwNo"`
	OrderID      string `json:"orderId"`
	PGCode       string `json:"pgCode"`
	Rate         string `json:"rate"`
	RefundAmount any    `json:"refundAmount"`
	Status       string `json:"status"`
	TradeTime    string `json:"tradeTime"`
}

type failureSignature struct {
	Balance      any    `json:"balance"`
	BankCode     string `json:"bankCode"`
	BankNo       string `json:"bankNo"`
	CurType      string `json:"curType"`
	MWNo         string `json:"mwNo"`
	OrderID      string `json:"orderId"`
	RefundAmount any    `json:"refundAmount"`
	Status       string `json:"status"`
}

type ticketingNotification struct {
	RetData successNotification `json:"retData"`
	SignMsg string              `json:"signMsg"`
}

func (s *WebhookService) ticketingPayload(record storage.Refund, rawCallback json.RawMessage, outcome string, balance any) (ticketingNotification, error) {
	var request CreateRequest
	if json.Unmarshal(record.Data, &request) != nil {
		return ticketingNotification{}, errors.New("invalid stored refund data")
	}
	var amount amountData
	if json.Unmarshal(record.AmountData, &amount) != nil {
		return ticketingNotification{}, errors.New("invalid stored refund amount data")
	}
	var callback XenditWebhook
	if json.Unmarshal(rawCallback, &callback) != nil {
		return ticketingNotification{}, errors.New("invalid stored Xendit callback")
	}
	data := successNotification{Balance: balance, BankCode: request.ReqData.Account.BankID,
		BankNo: mask(request.ReqData.Account.AccountNo, 4), CurType: "360", MWNo: record.ID,
		OrderID: value(record.RefundGANumber), RefundAmount: amount.Amount, Status: statusWording(storage.RefundFail)}
	var signInput any
	if outcome == "success" {
		data.Fee, data.PGCode, data.Rate = amount.Fee, "xendit", "0%+"+strconv.FormatInt(amount.Fee, 10)
		data.Status, data.TradeTime = statusWording(storage.RefundSuccess), jakartaTime(callback.Data.Updated)
		signInput = data
	} else {
		data.Fee, data.PGCode, data.Rate, data.TradeTime = "", "", "", ""
		signInput = failureSignature{Balance: data.Balance, BankCode: data.BankCode, BankNo: data.BankNo,
			CurType: data.CurType, MWNo: data.MWNo, OrderID: data.OrderID,
			RefundAmount: data.RefundAmount, Status: data.Status}
	}
	encoded, _ := json.Marshal(signInput)
	signature, err := s.signer.Sign(encoded)
	if err != nil {
		return ticketingNotification{}, err
	}
	return ticketingNotification{RetData: data, SignMsg: signature}, nil
}

func callbackDedupeKey(callback XenditWebhook) string {
	key := strings.Join([]string{callback.Event, callback.BusinessID, callback.Data.ID,
		strings.ToLower(callback.Data.Status), callback.Data.Updated, callback.Data.FailureCode}, "\x00")
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}

func webhookError(status int, message string, cause error) error {
	return &WebhookError{HTTPStatus: status, Payload: map[string]any{"status": status, "message": message}, Cause: cause}
}

func badWebhookRequest(problems []string, cause error) error {
	return &WebhookError{HTTPStatus: 400, Payload: map[string]any{"message": problems, "error": "Bad Request", "statusCode": 400}, Cause: cause}
}

func notifyURL(raw json.RawMessage) string {
	var request CreateRequest
	_ = json.Unmarshal(raw, &request)
	return request.ReqData.Invoice.NotifyURL
}

func value(input *string) string {
	if input == nil {
		return ""
	}
	return *input
}
