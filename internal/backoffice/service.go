package backoffice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/storage"
)

type Store interface {
	ListRefunds(context.Context, []storage.RefundFilter) ([]storage.RefundAdminRecord, error)
	GetRefundAdmin(context.Context, string, bool) (storage.RefundAdminRecord, error)
	ListRefundLogs(context.Context, []storage.RefundLogFilter) ([]storage.RefundLog, error)
	ConfigurationInt(context.Context, string, int) (int, error)
	GetRefundByGANumber(context.Context, string) (storage.Refund, error)
	GetBankDataByID(context.Context, string) (storage.BankData, error)
	ReserveRefundRetry(context.Context, string, int, int, time.Time, json.RawMessage) (storage.Refund, error)
	CompleteRefundRetry(context.Context, string, *string, json.RawMessage) error
	FailRefundRetry(context.Context, string, string, json.RawMessage) error
}

type CredentialProvider interface {
	XenditCredential(context.Context, string) (coreclient.XenditCredential, error)
}

type Decryptor interface {
	Decrypt(context.Context, encryptor.Ciphertext) (string, error)
}

type Service struct {
	store       Store
	credentials CredentialProvider
	decryptor   Decryptor
	provider    refund.PayoutProvider
	environment string
}

func New(store Store, credentials CredentialProvider, decryptor Decryptor, provider refund.PayoutProvider, environment string) *Service {
	return &Service{store: store, credentials: credentials, decryptor: decryptor, provider: provider, environment: environment}
}

func (s *Service) List(ctx context.Context, filters []storage.RefundFilter) ([]Refund, error) {
	records, err := s.store.ListRefunds(ctx, filters)
	if err != nil {
		return nil, err
	}
	result := make([]Refund, 0, len(records))
	for _, record := range records {
		result = append(result, mapRefund(record, false, true))
	}
	return result, nil
}

func (s *Service) View(ctx context.Context, id string, relationDetail bool) (*Refund, error) {
	record, err := s.store.GetRefundAdmin(ctx, id, relationDetail)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := mapRefund(record, true, false)
	return &result, nil
}

func (s *Service) Logs(ctx context.Context, filters []storage.RefundLogFilter) ([]RefundLog, error) {
	records, err := s.store.ListRefundLogs(ctx, filters)
	if err != nil {
		return nil, err
	}
	result := make([]RefundLog, 0, len(records))
	for _, record := range records {
		result = append(result, RefundLog{
			ID: record.ID, Type: record.Type, Location: record.Location, Detail: record.Detail,
			Msg: record.Message, Notes: record.Notes, CreatedAt: record.CreatedAt,
			UpdatedAt: record.UpdatedAt, DeletedAt: record.DeletedAt,
		})
	}
	return result, nil
}

func (s *Service) Retry(ctx context.Context, refundGANumber string) error {
	record, err := s.store.GetRefundByGANumber(ctx, refundGANumber)
	if err != nil {
		return errors.New("Refund not found")
	}
	if record.Status == nil || *record.Status != storage.RefundFail {
		return errors.New("Retry allowed only for fail status")
	}
	maxAttempts, err := s.store.ConfigurationInt(ctx, "REFUND_TRY_COUNT", 1)
	if err != nil {
		return err
	}
	var attempts []string
	_ = json.Unmarshal(record.RetryAttempt, &attempts)
	if len(attempts) >= maxAttempts {
		return errors.New("Max Attempt to retry")
	}
	if record.BankDataID == nil {
		return errors.New("Refund bank data not found")
	}
	bank, err := s.store.GetBankDataByID(ctx, *record.BankDataID)
	if err != nil {
		return errors.New("Refund bank data not found")
	}
	var ciphertext encryptor.Ciphertext
	if json.Unmarshal(bank.AccountNumberEnc, &ciphertext) != nil {
		return errors.New("Invalid encrypted bank account")
	}
	accountNumber, err := s.decryptor.Decrypt(ctx, ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt refund bank account: %w", err)
	}
	var stored struct {
		ReqData struct {
			Account struct {
				Name string `json:"name"`
			} `json:"account"`
			Invoice struct {
				Reason string `json:"reason"`
			} `json:"invoice"`
		} `json:"reqData"`
	}
	var bankData struct {
		BankCode string `json:"bankCode"`
		BankName string `json:"bankName"`
	}
	var amountData struct {
		Amount int64 `json:"amount"`
	}
	if json.Unmarshal(record.Data, &stored) != nil || json.Unmarshal(record.BankData, &bankData) != nil || json.Unmarshal(record.AmountData, &amountData) != nil {
		return errors.New("Invalid stored refund data")
	}
	if bankData.BankName == "" {
		bankData.BankName = stored.ReqData.Account.Name
	}
	referenceID := refundGANumber + "-" + strconv.Itoa(len(attempts)+1)
	payout := refund.PayoutRequest{
		ReferenceID: referenceID, ChannelCode: bankData.BankCode,
		ChannelProperties: refund.PayoutChannelProperties{AccountNumber: accountNumber, AccountHolderName: bankData.BankName},
		Amount:            amountData.Amount, Description: stored.ReqData.Invoice.Reason, Currency: "IDR", IdempotencyKey: refundGANumber,
	}
	intent := payout
	intent.ChannelProperties.AccountNumber = mask(accountNumber, 4)
	intentJSON, _ := json.Marshal(intent)
	if _, err := s.store.ReserveRefundRetry(ctx, record.ID, len(attempts), maxAttempts, time.Now(), intentJSON); err != nil {
		switch {
		case errors.Is(err, storage.ErrRetryLimit):
			return errors.New("Max Attempt to retry")
		case errors.Is(err, storage.ErrRetryNotAllowed):
			return errors.New("Retry allowed only for fail status")
		default:
			return err
		}
	}
	credential, err := s.credentials.XenditCredential(ctx, s.environment)
	if err != nil {
		_ = s.store.FailRefundRetry(ctx, record.ID, "Failed Xendit disbursement", nil)
		return err
	}
	providerResult := s.provider.Create(ctx, credential.SecretKey, refundGANumber, payout)
	safeResponse := sanitizeProviderJSON(providerResult.Data)
	if providerResult.Err != nil || providerResult.Status != 200 {
		_ = s.store.FailRefundRetry(ctx, record.ID, "Failed Xendit disbursement", safeResponse)
		return errors.New("Failed Xendit disbursement")
	}
	var response map[string]any
	_ = json.Unmarshal(providerResult.Data, &response)
	var payoutID *string
	if id, ok := response["id"].(string); ok && id != "" {
		payoutID = &id
	}
	return s.store.CompleteRefundRetry(ctx, record.ID, payoutID, safeResponse)
}

type Refund struct {
	ID                   string                `json:"id"`
	RefundID             *string               `json:"refundId"`
	RefundStatus         *storage.RefundStatus `json:"refundStatus"`
	RefundAmount         any                   `json:"refundAmount"`
	RefundAmountData     json.RawMessage       `json:"refundAmountData"`
	RefundData           json.RawMessage       `json:"refundData"`
	RefundReason         *string               `json:"refundReason"`
	RejectReason         *string               `json:"rejectReason"`
	RejectBy             *string               `json:"rejectBy"`
	ApprovalFinBy        *string               `json:"approvalFinBy"`
	ApprovalRBDBy        *string               `json:"approvalRbdBy"`
	ApprovalFinAt        *time.Time            `json:"approvalFinAt"`
	ApprovalRBDAt        *time.Time            `json:"approvalRbdAt"`
	RejectAt             *time.Time            `json:"rejectAt"`
	RefundBankData       json.RawMessage       `json:"refundBankData"`
	RefundDate           *time.Time            `json:"refundDate"`
	RequestData          json.RawMessage       `json:"requestData"`
	RetryAttempt         json.RawMessage       `json:"retryAttempt"`
	RetryDate            *time.Time            `json:"retryDate"`
	TargetRefundDate     *time.Time            `json:"targetRefundDate"`
	RefundExecuteData    json.RawMessage       `json:"refundExecuteData"`
	NotifLog             json.RawMessage       `json:"notifLog"`
	DisbursementID       *string               `json:"disbursementId"`
	DisbursementResponse json.RawMessage       `json:"disbursementResponse"`
	BankDataID           *string               `json:"bankDataId"`
	RefundDetail         *RefundDetail         `json:"refundDetail,omitempty"`
	WebhookCalls         []WebhookCall         `json:"webhookCalls,omitempty"`
	CreatedAt            time.Time             `json:"createdAt"`
	UpdatedAt            time.Time             `json:"updatedAt"`
	DeletedAt            *time.Time            `json:"deletedAt,omitempty"`
}

type RefundDetail struct {
	ID           string     `json:"id"`
	RefundMwID   *string    `json:"refundMwId"`
	Email        *string    `json:"email"`
	PhoneNumber  *string    `json:"phoneNumber"`
	Reason       *string    `json:"reason"`
	RefundID     *string    `json:"refundId"`
	RefundAmount any        `json:"refundAmount"`
	TicketOffice *string    `json:"ticketOffice"`
	TicketData   []Ticket   `json:"ticketData"`
	CreatedAt    time.Time  `json:"createdAt"`
	UpdatedAt    time.Time  `json:"updatedAt"`
	DeletedAt    *time.Time `json:"deletedAt,omitempty"`
}

type Ticket struct {
	ID               string     `json:"id"`
	ArrivalStation   *string    `json:"arrivalStation"`
	CarsNumber       *string    `json:"carsNumber"`
	DepartureStation *string    `json:"departureStation"`
	IdentityNumber   *string    `json:"identityNumber"`
	IdentityType     *string    `json:"identityType"`
	Name             *string    `json:"name"`
	OrderNumber      *string    `json:"orderNumber"`
	PurchasePrice    any        `json:"purchasePrice"`
	SeatNumber       *string    `json:"seatNumber"`
	TicketClass      *string    `json:"ticketClass"`
	TicketNumber     *string    `json:"ticketNumber"`
	DepartureDate    *time.Time `json:"departureDate"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
	DeletedAt        *time.Time `json:"deletedAt,omitempty"`
}

type WebhookCall struct {
	ID             string          `json:"id"`
	RefundRef      string          `json:"refundRef"`
	Source         string          `json:"source"`
	Payload        json.RawMessage `json:"payload"`
	Response       json.RawMessage `json:"response"`
	ResponseStatus *int            `json:"responseStatus"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type RefundLog struct {
	ID        string     `json:"id"`
	Type      string     `json:"type"`
	Location  string     `json:"location"`
	Detail    string     `json:"detail"`
	Msg       string     `json:"msg"`
	Notes     string     `json:"notes"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
	DeletedAt *time.Time `json:"deletedAt,omitempty"`
}

func mapRefund(record storage.RefundAdminRecord, includeDetail, includeWebhooks bool) Refund {
	r := record.Refund
	result := Refund{ID: r.ID, RefundID: r.RefundGANumber, RefundStatus: r.Status, RefundAmount: number(r.Amount),
		RefundAmountData: raw(r.AmountData), RefundData: raw(r.Data), RefundReason: r.Reason,
		RejectReason: r.RejectReason, RejectBy: r.RejectBy, ApprovalFinBy: r.ApprovalFinanceBy,
		ApprovalRBDBy: r.ApprovalRBDBy, ApprovalFinAt: r.ApprovalFinanceAt, ApprovalRBDAt: r.ApprovalRBDAt,
		RejectAt: r.RejectAt, RefundBankData: raw(r.BankData), RefundDate: r.RefundDate,
		RequestData: raw(r.RequestData), RetryAttempt: raw(r.RetryAttempt), RetryDate: r.RetryDate,
		TargetRefundDate: r.TargetRefundDate, RefundExecuteData: raw(r.ExecuteData), NotifLog: raw(r.NotificationLog),
		DisbursementID: r.DisbursementID, DisbursementResponse: raw(r.DisbursementResponse), BankDataID: r.BankDataID,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, DeletedAt: r.DeletedAt}
	if includeDetail && record.Detail != nil {
		d := record.Detail
		detail := RefundDetail{ID: d.ID, RefundMwID: d.RefundID, Email: d.Email, PhoneNumber: d.PhoneNumber,
			Reason: d.Reason, RefundID: d.RefundGANumber, RefundAmount: number(d.Amount), TicketOffice: d.TicketOffice,
			TicketData: make([]Ticket, 0, len(record.Tickets)), CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt, DeletedAt: d.DeletedAt}
		for _, t := range record.Tickets {
			detail.TicketData = append(detail.TicketData, Ticket{ID: t.ID, ArrivalStation: t.ArrivalStation,
				CarsNumber: t.CarsNumber, DepartureStation: t.DepartureStation, IdentityNumber: t.IdentityNumber,
				IdentityType: t.IdentityType, Name: t.Name, OrderNumber: t.OrderNumber, PurchasePrice: number(t.PurchasePrice),
				SeatNumber: t.SeatNumber, TicketClass: t.TicketClass, TicketNumber: t.TicketNumber,
				DepartureDate: t.DepartureDate, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt, DeletedAt: t.DeletedAt})
		}
		result.RefundDetail = &detail
	}
	if includeWebhooks {
		result.WebhookCalls = make([]WebhookCall, 0, len(record.WebhookCalls))
		for _, w := range record.WebhookCalls {
			result.WebhookCalls = append(result.WebhookCalls, WebhookCall{ID: w.ID, RefundRef: w.RefundReference,
				Source: w.Source, Payload: raw(w.Payload), Response: raw(w.Response), ResponseStatus: w.ResponseStatus,
				CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt})
		}
	}
	return result
}

func number(value *string) any {
	if value == nil {
		return nil
	}
	return json.Number(*value)
}

func raw(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("null")
	}
	return value
}

func mask(value string, visible int) string {
	runes := []rune(value)
	if len(runes) <= visible {
		return strings.Repeat("*", len(runes))
	}
	return strings.Repeat("*", len(runes)-visible) + string(runes[len(runes)-visible:])
}

func sanitizeProviderJSON(rawValue json.RawMessage) json.RawMessage {
	if len(rawValue) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(rawValue, &value) != nil {
		return json.RawMessage(`{"message":"invalid provider response"}`)
	}
	if object, ok := value.(map[string]any); ok {
		if properties, ok := object["channel_properties"].(map[string]any); ok {
			for _, key := range []string{"account_number", "accountNo"} {
				if account, ok := properties[key].(string); ok {
					properties[key] = mask(account, 4)
				}
			}
		}
	}
	encoded, _ := json.Marshal(value)
	return encoded
}
