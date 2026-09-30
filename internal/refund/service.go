package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/storage"
)

type Store interface {
	RefundExists(context.Context, string) (bool, error)
	GetEnabledRefundBankByXenditCode(context.Context, string) (storage.RefundBank, error)
	FindBankData(context.Context, string, string) (storage.BankData, error)
	CreateRefund(context.Context, storage.RefundInsert) (storage.Refund, error)
	GetRefundByGANumber(context.Context, string) (storage.Refund, error)
	AppendRefundRequestIntent(context.Context, string, json.RawMessage) (storage.Refund, error)
	RecordRefundPayoutSuccess(context.Context, string, *string, json.RawMessage) (storage.Refund, error)
	RecordRefundPayoutFailure(context.Context, string, string, json.RawMessage) error
	MarkRefundFailedIfPresent(context.Context, string, string) error
	InsertRefundLog(context.Context, storage.RefundLog) error
}

type CredentialProvider interface {
	XenditCredential(context.Context, string) (coreclient.XenditCredential, error)
}
type Encryptor interface {
	Encrypt(context.Context, string) (encryptor.Ciphertext, error)
	BlindIndex(context.Context, string) (string, error)
}
type Signer interface{ Sign([]byte) (string, error) }

type Config struct {
	Environment      string
	FeeFix, PPNValue int64
}

type Service struct {
	store       Store
	credentials CredentialProvider
	encryptor   Encryptor
	signer      Signer
	provider    PayoutProvider
	cfg         Config
	logger      *slog.Logger
}

func New(store Store, credentials CredentialProvider, encrypt Encryptor, signer Signer, provider PayoutProvider, cfg Config, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: store, credentials: credentials, encryptor: encrypt, signer: signer, provider: provider, cfg: cfg, logger: logger}
}

func (s *Service) Create(ctx context.Context, request CreateRequest) (Response, error) {
	orderID := request.ReqData.Invoice.OrderID
	credential, err := s.credentials.XenditCredential(ctx, s.cfg.Environment)
	if err != nil {
		return Response{}, s.createFailure(ctx, orderID, "get Xendit credential", err, "", nil)
	}
	exists, err := s.store.RefundExists(ctx, orderID)
	if err != nil {
		return Response{}, s.createFailure(ctx, orderID, "check duplicate refund", err, "", nil)
	}
	if exists {
		return Response{}, s.createFailure(ctx, orderID, "Duplicate Request", storage.ErrDuplicateRefund, "", nil)
	}

	bankCode := request.ReqData.Account.BankID
	if len(strings.Split(bankCode, "_")) == 1 {
		bankCode = "ID_" + bankCode
	}
	if _, err = s.store.GetEnabledRefundBankByXenditCode(ctx, bankCode); err != nil {
		return Response{}, s.createFailure(ctx, orderID, request.ReqData.Account.BankID+" : Bank code not found", err, "", nil)
	}
	hash, err := s.encryptor.BlindIndex(ctx, request.ReqData.Account.AccountNo)
	if err != nil {
		return Response{}, s.createFailure(ctx, orderID, "blind-index account", err, "", nil)
	}
	var bankDataID *string
	var bankDataCreate *storage.BankDataInsert
	existing, findErr := s.store.FindBankData(ctx, bankCode, hash)
	switch {
	case findErr == nil:
		bankDataID = &existing.ID
	case !errors.Is(findErr, storage.ErrNotFound):
		return Response{}, s.createFailure(ctx, orderID, "lookup bank data", findErr, "", nil)
	default:
		ciphertext, encryptErr := s.encryptor.Encrypt(ctx, request.ReqData.Account.AccountNo)
		if encryptErr != nil {
			return Response{}, s.createFailure(ctx, orderID, "encrypt account", encryptErr, "", nil)
		}
		encoded, _ := json.Marshal(ciphertext)
		bankDataCreate = &storage.BankDataInsert{BankCode: bankCode, AccountNumberEnc: encoded, AccountNumberHash: hash, Status: storage.AccountPending, Result: storage.AccountResultPending, LastCheckAt: time.Now()}
	}

	amount := *request.ReqData.Invoice.RefundAmount
	amountData := amountBreakdown(amount, s.cfg.FeeFix, s.cfg.PPNValue)
	maskedRequest := request
	maskedRequest.ReqData.Account.AccountNo = mask(request.ReqData.Account.AccountNo, 4)
	maskedRequest.ReqData.Account.IDNo = mask(request.ReqData.Account.IDNo, 4)
	requestJSON, _ := json.Marshal(maskedRequest)
	bankJSON, _ := json.Marshal(map[string]string{"bankName": request.ReqData.Account.Name, "bankNumber": mask(request.ReqData.Account.AccountNo, 4), "bankCode": bankCode})
	amountJSON, _ := json.Marshal(amountData)
	status := storage.RefundPendingChecking
	var detail *storage.RefundDetailInsert
	if request.TicketCall != nil && *request.TicketCall == 0 {
		status = storage.RefundRBDApproval
	} else {
		detail = &storage.RefundDetailInsert{ID: storage.NewID(), RefundID: "", RefundGANumber: orderID, Amount: "0"}
	}
	refundID := storage.NewID()
	if detail != nil {
		detail.RefundID = refundID
	}
	created, err := s.store.CreateRefund(ctx, storage.RefundInsert{
		ID: refundID, RefundGANumber: orderID, Status: status, Amount: strconv.FormatInt(amount, 10),
		AmountData: amountJSON, Data: requestJSON, Reason: request.ReqData.Invoice.Reason, BankData: bankJSON,
		RequestData: json.RawMessage(`[]`), BankDataID: bankDataID, BankDataCreate: bankDataCreate, Detail: detail,
	})
	if err != nil {
		return Response{}, s.createFailure(ctx, orderID, errorMessage(err), err, "", nil)
	}

	payout := PayoutRequest{ReferenceID: orderID, ChannelCode: bankCode,
		ChannelProperties: PayoutChannelProperties{AccountNumber: request.ReqData.Account.AccountNo, AccountHolderName: request.ReqData.Account.Name},
		Amount:            amount, Description: request.ReqData.Invoice.Reason, Currency: "IDR", IdempotencyKey: orderID}
	intent := payout
	intent.ChannelProperties.AccountNumber = mask(intent.ChannelProperties.AccountNumber, 4)
	intentJSON, _ := json.Marshal(intent)
	if _, err = s.store.AppendRefundRequestIntent(ctx, created.ID, intentJSON); err != nil {
		return Response{}, s.createFailure(ctx, orderID, "persist payout intent", err, created.ID, nil)
	}
	providerResult := s.provider.Create(ctx, credential.SecretKey, orderID, payout)
	safeProvider := sanitizeProviderJSON(providerResult.Data)
	if providerResult.Err != nil || providerResult.Status != 200 {
		cause := providerResult.Err
		if cause == nil {
			cause = errors.New("Failed Xendit disbursement")
		}
		return Response{}, s.createFailure(ctx, orderID, "Failed Xendit disbursement", cause, created.ID, safeProvider)
	}
	var providerData map[string]any
	if json.Unmarshal(providerResult.Data, &providerData) != nil {
		providerData = map[string]any{}
	}
	var payoutID *string
	if value, ok := providerData["id"].(string); ok && value != "" {
		payoutID = &value
	}
	if _, err = s.store.RecordRefundPayoutSuccess(ctx, created.ID, payoutID, safeProvider); err != nil {
		return Response{}, s.createFailure(ctx, orderID, "persist Xendit disbursement result", err, created.ID, nil)
	}
	invoice := CreateInvoice{OrderID: orderID, Status: statusWording(storage.RefundPendingDisbursement)}
	response, err := s.signResponse(invoice, "Success")
	if err != nil {
		return Response{}, s.createFailure(ctx, orderID, "sign refund response", err, created.ID, nil)
	}
	return response, nil
}

func (s *Service) Status(ctx context.Context, request StatusRequest) (Response, error) {
	credential, err := s.credentials.XenditCredential(ctx, s.cfg.Environment)
	if err != nil {
		return Response{}, statusFailure(err)
	}
	record, err := s.store.GetRefundByGANumber(ctx, request.ReqData.Invoice.OrderID)
	if errors.Is(err, storage.ErrNotFound) {
		return Response{}, &PublicError{HTTPStatus: 404, Payload: map[string]any{"message": "Refund not found", "statusCode": 404}, Cause: err}
	}
	if err != nil {
		return Response{}, statusFailure(err)
	}
	var stored CreateRequest
	if json.Unmarshal(record.Data, &stored) != nil {
		return Response{}, statusFailure(errors.New("invalid stored refund data"))
	}
	var payout map[string]any
	if record.DisbursementID != nil && *record.DisbursementID != "" {
		result := s.provider.Status(ctx, credential.SecretKey, *record.DisbursementID)
		if result.Err != nil || result.Status < 200 || result.Status >= 300 {
			if result.Err != nil {
				return Response{}, statusFailure(result.Err)
			}
			return Response{}, statusFailure(fmt.Errorf("Xendit payout status HTTP %d", result.Status))
		}
		if json.Unmarshal(result.Data, &payout) != nil {
			return Response{}, statusFailure(errors.New("invalid Xendit payout status"))
		}
	}
	invoice := StatusInvoice{Balance: nil, BankCode: stored.ReqData.Account.BankID, CurType: "360",
		Fee: strconv.FormatInt(s.cfg.FeeFix, 10), MWNo: record.ID, OrderID: stored.ReqData.Invoice.OrderID,
		PGCode: "xendit", Rate: strconv.FormatInt(s.cfg.FeeFix, 10), RefundAmount: numberValue(record.Amount), Status: statusWordingValue(record.Status)}
	if payout != nil {
		invoice.Balance = payout["amount"]
		if properties, ok := payout["channel_properties"].(map[string]any); ok {
			if account, ok := properties["account_number"].(string); ok && account != "" {
				masked := mask(account, 4)
				invoice.BankNo = &masked
			}
		}
		if updated, ok := payout["updated"].(string); ok {
			if formatted := jakartaTime(updated); formatted != "" {
				invoice.TradeTime = &formatted
			}
		}
	}
	invoice.Comment = record.RejectReason
	return s.signResponse(invoice, "success")
}

func (s *Service) signResponse(invoice any, message string) (Response, error) {
	payload, err := json.Marshal(struct {
		Invoice any `json:"invoice"`
	}{Invoice: invoice})
	if err != nil {
		return Response{}, err
	}
	signature, err := s.signer.Sign(payload)
	if err != nil {
		return Response{}, err
	}
	return Response{RetCode: 0, RetMsg: message, RetData: ResponseData{Invoice: invoice}, SignMsg: signature}, nil
}

func (s *Service) createFailure(ctx context.Context, orderID, detail string, cause error, refundID string, response json.RawMessage) error {
	duplicate := errors.Is(cause, storage.ErrDuplicateRefund)
	_ = s.store.InsertRefundLog(ctx, storage.RefundLog{Type: "api", Location: "/api/v2/transfer", Detail: detail, Message: detail, Notes: orderID})
	if !duplicate {
		if refundID != "" {
			_ = s.store.RecordRefundPayoutFailure(ctx, refundID, detail, response)
		} else {
			_ = s.store.MarkRefundFailedIfPresent(ctx, orderID, detail)
		}
	}
	s.logger.ErrorContext(ctx, "refund create failed", "order_id", orderID, "error", cause)
	return &PublicError{HTTPStatus: map[bool]int{true: 409, false: 500}[duplicate], Payload: map[string]any{"retCode": -1, "retMsg": "Failed to process refund request"}, Cause: cause}
}

func statusFailure(cause error) error {
	return &PublicError{HTTPStatus: 500, Payload: map[string]any{"retCode": -1, "retMsg": "Failed to retrieve refund status"}, Cause: cause}
}

type amountData struct {
	Amount         int64 `json:"amount"`
	Fee            int64 `json:"fee"`
	AmountAfterFee int64 `json:"AmountAfterFee"`
	Tax            int64 `json:"tax"`
	TotalAmount    int64 `json:"totalAmount"`
}

func amountBreakdown(amount, fee, ppn int64) amountData {
	tax := (fee*ppn + 99) / 100
	return amountData{Amount: amount, Fee: fee, AmountAfterFee: amount + fee, Tax: tax, TotalAmount: amount + fee + tax}
}

func statusWording(status storage.RefundStatus) string {
	return map[storage.RefundStatus]string{storage.RefundRBDApproval: "RBD approval", storage.RefundFinanceApproval: "Finance approval", storage.RefundPendingDisbursement: "PG process", storage.RefundSuccess: "success", storage.RefundReject: "reject", storage.RefundFail: "fail", storage.RefundDone: "done", storage.RefundOnHold: "onHold", storage.RefundCancel: "cancel", storage.RefundRetry: "retry", storage.RefundPendingChecking: "pending Checking to Ticketing"}[status]
}
func statusWordingValue(status *storage.RefundStatus) string {
	if status == nil {
		return ""
	}
	return statusWording(*status)
}
func mask(value string, visible int) string {
	runes := []rune(value)
	if len(runes) <= visible {
		return strings.Repeat("*", len(runes))
	}
	return strings.Repeat("*", len(runes)-visible) + string(runes[len(runes)-visible:])
}
func numberValue(value *string) any {
	if value == nil {
		return nil
	}
	// node-postgres returns NUMERIC columns as strings; TypeORM preserves that
	// runtime value despite the entity's TypeScript annotation.
	return *value
}
func errorMessage(err error) string {
	if errors.Is(err, storage.ErrDuplicateRefund) {
		return "Duplicate Request"
	}
	return err.Error()
}

func sanitizeProviderJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		encoded, _ := json.Marshal(map[string]any{"message": "Unparseable provider response"})
		return encoded
	}
	maskSensitive(value)
	encoded, _ := json.Marshal(value)
	return encoded
}
func maskSensitive(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
			if normalized == "accountnumber" || normalized == "accountno" || normalized == "banknumber" || normalized == "bankno" || normalized == "idno" || normalized == "identitynumber" {
				if text, ok := child.(string); ok {
					typed[key] = mask(text, 4)
				}
			} else {
				maskSensitive(child)
			}
		}
	case []any:
		for _, child := range typed {
			maskSensitive(child)
		}
	}
}
func jakartaTime(raw string) string {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return ""
	}
	location, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		return ""
	}
	return parsed.In(location).Format("20060102150405")
}
