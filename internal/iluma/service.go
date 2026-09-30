package iluma

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/storage"
)

type Store interface {
	GetRefundBankByXenditCode(context.Context, string) (storage.RefundBank, error)
	FindBankData(context.Context, string, string) (storage.BankData, error)
	GetOrCreateBankData(context.Context, storage.BankDataInsert) (storage.BankData, bool, error)
	GetBankDataByID(context.Context, string) (storage.BankData, error)
	FindBankDataByRequestID(context.Context, string) (storage.BankData, error)
	SetBankDataRequestID(context.Context, string, string) error
	CompleteBankData(context.Context, string, storage.AccountResult, json.RawMessage, *string, *string) (bool, error)
	ExpireBankData(context.Context, string) (bool, error)
	InsertIlumaCallLog(context.Context, storage.IlumaCallLog) error
	UpdateIlumaCallback(context.Context, string, json.RawMessage, time.Time) (bool, error)
}

type Encryptor interface {
	Encrypt(context.Context, string) (encryptor.Ciphertext, error)
	BlindIndex(context.Context, string) (string, error)
}

type ResponseSigner interface {
	Sign([]byte) (string, error)
}

type EventPublisher interface {
	PublishEvent(context.Context, string, any, string) error
}

type Provider interface {
	Validate(context.Context, string, string) HTTPResult
	Result(context.Context, string) HTTPResult
}

type Config struct {
	BaseURL  string
	TTLDays  int
	MaxWait  time.Duration
	MinSleep time.Duration
	MaxSleep time.Duration
}

type Service struct {
	store     Store
	encryptor Encryptor
	signer    ResponseSigner
	publisher EventPublisher
	provider  Provider
	cfg       Config
	logger    *slog.Logger
	now       func() time.Time
}

type CheckAccountRequest struct {
	ReqData struct {
		Account Account `json:"account"`
	} `json:"reqData"`
	SignMsg string `json:"signMsg"`
}

type Account struct {
	BankID      string `json:"bankId"`
	AccountNo   string `json:"accountNo"`
	AccountType string `json:"accountType"`
	IDNo        string `json:"idNo"`
	IDType      string `json:"idType"`
	Name        string `json:"name"`
}

type CheckAccountResponse struct {
	RetCode   int               `json:"retCode"`
	RetMsg    string            `json:"retMsg,omitempty"`
	Message   string            `json:"message,omitempty"`
	ErrorCode string            `json:"errorCode,omitempty"`
	RetData   *CheckAccountData `json:"retData,omitempty"`
	SignMsg   string            `json:"signMsg,omitempty"`
}

type CheckAccountData struct {
	Status string `json:"status"`
}

type PollRequest struct {
	RequestID  string `json:"requestId"`
	BankDataID string `json:"bankDataId"`
}

type normalizedResponse struct {
	ID                      any               `json:"id"`
	Status                  string            `json:"status"`
	BankCode                any               `json:"bankCode"`
	BankAccountNumberMasked any               `json:"bankAccountNumberMasked"`
	Created                 any               `json:"created"`
	Updated                 any               `json:"updated"`
	ReferenceID             any               `json:"referenceId"`
	Result                  *normalizedResult `json:"result"`
	FailureReason           any               `json:"failureReason"`
	Raw                     any               `json:"raw"`
}

type normalizedResult struct {
	IsFound           any `json:"isFound"`
	IsVirtualAccount  any `json:"isVirtualAccount"`
	AccountHolderName any `json:"accountHolderName"`
	NeedReview        any `json:"needReview"`
}

func NewService(store Store, encrypt Encryptor, signer ResponseSigner, publisher EventPublisher, provider Provider, cfg Config, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: store, encryptor: encrypt, signer: signer, publisher: publisher, provider: provider, cfg: cfg, logger: logger, now: time.Now}
}

func ValidateCheckAccount(request CheckAccountRequest) []string {
	var problems []string
	fields := []struct{ name, value string }{
		{"signMsg", request.SignMsg}, {"reqData.account.bankId", request.ReqData.Account.BankID},
		{"reqData.account.accountNo", request.ReqData.Account.AccountNo}, {"reqData.account.accountType", request.ReqData.Account.AccountType},
		{"reqData.account.idNo", request.ReqData.Account.IDNo}, {"reqData.account.idType", request.ReqData.Account.IDType},
		{"reqData.account.name", request.ReqData.Account.Name},
	}
	for _, field := range fields {
		if field.value == "" {
			problems = append(problems, field.name+" should not be empty")
		}
	}
	return problems
}

func (s *Service) CheckAccount(ctx context.Context, request CheckAccountRequest) (CheckAccountResponse, error) {
	bank, err := s.store.GetRefundBankByXenditCode(ctx, request.ReqData.Account.BankID)
	if errors.Is(err, storage.ErrNotFound) || (err == nil && bank.Status == storage.BankDisabled) {
		return CheckAccountResponse{RetCode: -1, RetMsg: "Bank code not found"}, nil
	}
	if err != nil {
		return CheckAccountResponse{}, err
	}
	accountNumber := request.ReqData.Account.AccountNo
	hash, err := s.encryptor.BlindIndex(ctx, accountNumber)
	if err != nil {
		return CheckAccountResponse{}, err
	}
	bankData, err := s.resolveBankData(ctx, bank, accountNumber, hash)
	if err != nil {
		return CheckAccountResponse{}, err
	}

	result := s.provider.Validate(ctx, dereference(bank.XenditCode), accountNumber)
	if result.Error != nil || result.Status != 200 {
		providerError := result.Error
		if providerError == nil {
			providerError = &ProviderError{Code: "ILUMA_ERROR", Message: "Iluma validation failed"}
		}
		return CheckAccountResponse{RetCode: -1, RetMsg: providerError.Message, ErrorCode: providerError.Code}, nil
	}
	if err := s.writeLog(ctx, s.validateURL(), "bank-validator", map[string]any{
		"bank_code": dereference(bank.XenditCode), "bank_account_number": mask(accountNumber, 4),
	}, result.Data); err != nil {
		return CheckAccountResponse{}, err
	}
	data, err := decodeObject(result.Data)
	if err != nil || stringValue(data["status"]) == "" {
		return CheckAccountResponse{RetCode: -1, RetMsg: "Invalid response from Iluma"}, nil
	}
	requestID := stringValue(data["id"])
	if requestID != "" {
		if err := s.store.SetBankDataRequestID(ctx, bankData.ID, requestID); err != nil {
			return CheckAccountResponse{}, err
		}
	}
	status, err := s.handleInitial(ctx, bankData, data, accountNumber, dereference(bank.XenditCode))
	if err != nil {
		return CheckAccountResponse{}, err
	}
	if status == "timeout" {
		status = "failed"
	}
	return s.signedResponse(status)
}

func (s *Service) resolveBankData(ctx context.Context, bank storage.RefundBank, accountNumber, hash string) (storage.BankData, error) {
	existing, err := s.store.FindBankData(ctx, dereference(bank.XenditCode), hash)
	if err == nil {
		// Node computes freshness but still revalidates downstream. Preserve it.
		_ = existing.LastCheckAt != nil && existing.LastCheckAt.After(s.now().AddDate(0, 0, -s.cfg.TTLDays))
		return existing, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return storage.BankData{}, err
	}
	ciphertext, err := s.encryptor.Encrypt(ctx, accountNumber)
	if err != nil {
		return storage.BankData{}, err
	}
	encoded, err := json.Marshal(ciphertext)
	if err != nil {
		return storage.BankData{}, err
	}
	record, _, err := s.store.GetOrCreateBankData(ctx, storage.BankDataInsert{
		BankCode: dereference(bank.XenditCode), AccountNumberEnc: encoded, AccountNumberHash: hash,
		Status: storage.AccountPending, Result: storage.AccountResultPending, LastCheckAt: s.now(),
	})
	return record, err
}

func (s *Service) handleInitial(ctx context.Context, bankData storage.BankData, data map[string]any, account, bankCode string) (string, error) {
	status := strings.ToLower(stringValue(data["status"]))
	switch status {
	case "pending":
		return s.pollUntilDone(ctx, stringValue(data["id"]), bankData.ID, mask(account, 4), bankCode)
	case "completed":
		normalized := normalize(data)
		final := computeFinal(normalized)
		if err := s.complete(ctx, bankData.ID, final, normalized, nil, nil); err != nil {
			return "", err
		}
		return final, nil
	default:
		return computeFinal(normalize(data)), nil
	}
}

func (s *Service) pollUntilDone(ctx context.Context, requestID, bankDataID, maskedAccount, bankCode string) (string, error) {
	started := s.now()
	sleep := s.cfg.MinSleep
	for s.now().Sub(started) < s.cfg.MaxWait {
		if err := sleepContext(ctx, sleep); err != nil {
			return "", err
		}
		current, err := s.store.GetBankDataByID(ctx, bankDataID)
		if err != nil {
			return "", err
		}
		if current.AccountStatus == storage.AccountCompleted {
			return accountResult(current), nil
		}
		if current.AccountStatus != storage.AccountPending {
			return accountResult(current), nil
		}
		result := s.provider.Result(ctx, requestID)
		if err := s.writeLog(ctx, s.resultURL(requestID), "get-result", map[string]any{
			"bank_code": nullableString(bankCode), "bank_account_number": nullableString(maskedAccount),
		}, resultEnvelope(result)); err != nil {
			return "", err
		}
		if result.Status == 200 {
			data, decodeErr := decodeObject(result.Data)
			if decodeErr == nil && strings.ToLower(stringValue(data["status"])) == "completed" {
				normalized := normalize(data)
				final := computeFinal(normalized)
				if err := s.complete(ctx, bankDataID, final, normalized, nil, nil); err != nil {
					return "", err
				}
				return final, nil
			}
		}
		sleep *= 2
		if sleep > s.cfg.MaxSleep {
			sleep = s.cfg.MaxSleep
		}
	}
	expired, err := s.store.ExpireBankData(ctx, bankDataID)
	if err != nil {
		return "", err
	}
	if !expired {
		current, getErr := s.store.GetBankDataByID(ctx, bankDataID)
		if getErr == nil && current.AccountStatus == storage.AccountCompleted {
			return accountResult(current), nil
		}
	}
	if s.publisher != nil {
		messageID := "refund.iluma.poll:" + requestID + ":" + bankDataID
		if publishErr := s.publisher.PublishEvent(ctx, "refund.iluma.poll", PollRequest{RequestID: requestID, BankDataID: bankDataID}, messageID); publishErr != nil {
			s.logger.ErrorContext(ctx, "Iluma fallback publish failed", "request_id", requestID, "bank_data_id", bankDataID, "error", publishErr)
		}
	}
	return "timeout", nil
}

// Poll is the asynchronous fallback. It deliberately exits for completed or
// expired rows, preserving the frozen Node behavior.
func (s *Service) Poll(ctx context.Context, request PollRequest) error {
	if request.RequestID == "" || request.BankDataID == "" {
		return nil
	}
	record, err := s.store.GetBankDataByID(ctx, request.BankDataID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return nil
	}
	if record.AccountStatus == storage.AccountCompleted || record.AccountStatus == storage.AccountExpired {
		return nil
	}
	result := s.provider.Result(ctx, request.RequestID)
	if err := s.writeLog(ctx, s.resultURL(request.RequestID), "get-result-worker", map[string]any{"bank_code": nil}, resultEnvelope(result)); err != nil {
		return nil
	}
	if result.Status != 200 {
		return nil
	}
	data, err := decodeObject(result.Data)
	if err != nil || stringValue(data["status"]) == "" {
		return nil
	}
	_, _ = s.pollUntilDone(ctx, request.RequestID, request.BankDataID, "", "")
	return nil
}

func (s *Service) Callback(ctx context.Context, raw json.RawMessage) (map[string]string, error) {
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return map[string]string{"message": "OK"}, nil
	}
	requestID := stringValue(data["id"])
	_, _ = s.store.UpdateIlumaCallback(ctx, requestID, raw, s.now())
	record, err := s.store.FindBankDataByRequestID(ctx, requestID)
	if errors.Is(err, storage.ErrNotFound) {
		return map[string]string{"message": "Ignored"}, nil
	}
	if err != nil {
		return map[string]string{"message": "OK"}, nil
	}
	if record.AccountStatus != storage.AccountPending {
		return map[string]string{"message": "Ignored"}, nil
	}
	normalized := normalize(data)
	final := "failed"
	if strings.ToLower(stringValue(data["status"])) == "completed" {
		final = computeFinal(normalized)
	}
	var failureCode, failureMessage *string
	if result, ok := data["result"].(map[string]any); ok {
		failureCode = stringPointer(result["error_code"])
		failureMessage = stringPointer(result["message"])
	}
	if err := s.complete(ctx, record.ID, final, normalized, failureCode, failureMessage); err != nil {
		return map[string]string{"message": "OK"}, nil
	}
	return map[string]string{"message": "OK"}, nil
}

func (s *Service) complete(ctx context.Context, id, final string, normalized normalizedResponse, failureCode, failureMessage *string) error {
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	result := storage.AccountResultFailed
	if final == "success" {
		result = storage.AccountResultSuccess
	}
	_, err = s.store.CompleteBankData(ctx, id, result, encoded, failureCode, failureMessage)
	return err
}

func (s *Service) signedResponse(status string) (CheckAccountResponse, error) {
	payload, _ := json.Marshal(struct {
		Status string `json:"status"`
	}{Status: status})
	signature, err := s.signer.Sign(payload)
	if err != nil {
		return CheckAccountResponse{}, err
	}
	return CheckAccountResponse{RetCode: 0, Message: "Success", RetData: &CheckAccountData{Status: status}, SignMsg: signature}, nil
}

func (s *Service) writeLog(ctx context.Context, endpoint, function string, payload any, response json.RawMessage) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.store.InsertIlumaCallLog(ctx, storage.IlumaCallLog{URL: endpoint, Method: "post", Function: function, Payload: payloadJSON, Response: response})
}

func (s *Service) validateURL() string {
	return strings.TrimRight(s.cfg.BaseURL, "/") + "/v1.2/identity/bank_account_validation_details"
}
func (s *Service) resultURL(id string) string { return s.validateURL() + "/" + id }

func resultEnvelope(result HTTPResult) json.RawMessage {
	var data any
	if json.Unmarshal(result.Data, &data) != nil {
		data = nil
	}
	value := map[string]any{"status": result.Status, "data": data}
	if result.Error != nil {
		value["error"] = result.Error
	}
	encoded, _ := json.Marshal(value)
	return encoded
}

func normalize(data map[string]any) normalizedResponse {
	result := normalizedResponse{
		ID: nullable(data["id"]), Status: strings.ToLower(stringValue(data["status"])),
		BankCode: nullable(data["bank_code"]), BankAccountNumberMasked: nil,
		Created: nullable(data["created"]), Updated: nullable(data["updated"]),
		ReferenceID: nullable(data["reference_id"]), FailureReason: nullable(data["failure_reason"]), Raw: data,
	}
	if providerResult, ok := data["result"].(map[string]any); ok {
		result.Result = &normalizedResult{
			IsFound: nullable(providerResult["is_found"]), IsVirtualAccount: nullable(providerResult["is_virtual_account"]),
			AccountHolderName: nullable(providerResult["account_holder_name"]), NeedReview: nullable(providerResult["need_review"]),
		}
	}
	return result
}

func computeFinal(value normalizedResponse) string {
	if value.Status != "completed" || value.Result == nil {
		return "failed"
	}
	found, foundOK := value.Result.IsFound.(bool)
	virtual, virtualOK := value.Result.IsVirtualAccount.(bool)
	if foundOK && virtualOK && found && !virtual {
		return "success"
	}
	return "failed"
}

func accountResult(record storage.BankData) string {
	if record.AccountResult != nil && *record.AccountResult == storage.AccountResultSuccess {
		return "success"
	}
	return "failed"
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func mask(value string, visible int) string {
	if value == "" {
		return value
	}
	runes := []rune(value)
	if visible > len(runes) {
		visible = len(runes)
	}
	return strings.Repeat("*", len(runes)-visible) + string(runes[len(runes)-visible:])
}

func dereference(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func stringPointer(value any) *string {
	result, ok := value.(string)
	if !ok || result == "" {
		return nil
	}
	return &result
}

func nullable(value any) any {
	if value == nil {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
