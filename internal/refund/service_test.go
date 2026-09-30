package refund

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/storage"
)

type refundStoreFake struct {
	exists           bool
	bank             storage.RefundBank
	bankData         *storage.BankData
	created          storage.RefundInsert
	record           storage.Refund
	intent           json.RawMessage
	providerResponse json.RawMessage
	failedReason     string
	logs             []storage.RefundLog
}

func (f *refundStoreFake) RefundExists(context.Context, string) (bool, error) { return f.exists, nil }
func (f *refundStoreFake) GetEnabledRefundBankByXenditCode(_ context.Context, code string) (storage.RefundBank, error) {
	if f.bank.ID == "" || f.bank.XenditCode == nil || *f.bank.XenditCode != code {
		return storage.RefundBank{}, storage.ErrNotFound
	}
	return f.bank, nil
}
func (f *refundStoreFake) FindBankData(context.Context, string, string) (storage.BankData, error) {
	if f.bankData == nil {
		return storage.BankData{}, storage.ErrNotFound
	}
	return *f.bankData, nil
}
func (f *refundStoreFake) CreateRefund(_ context.Context, in storage.RefundInsert) (storage.Refund, error) {
	if f.exists {
		return storage.Refund{}, storage.ErrDuplicateRefund
	}
	f.created = in
	status := in.Status
	amount := in.Amount
	ga := in.RefundGANumber
	bankID := in.BankDataID
	if bankID == nil && in.BankDataCreate != nil {
		id := "bank-data-new"
		bankID = &id
	}
	f.record = storage.Refund{ID: in.ID, RefundGANumber: &ga, Status: &status, Amount: &amount, AmountData: in.AmountData, Data: in.Data, BankData: in.BankData, RequestData: in.RequestData, BankDataID: bankID}
	return f.record, nil
}
func (f *refundStoreFake) GetRefundByGANumber(context.Context, string) (storage.Refund, error) {
	if f.record.ID == "" {
		return storage.Refund{}, storage.ErrNotFound
	}
	return f.record, nil
}
func (f *refundStoreFake) AppendRefundRequestIntent(_ context.Context, _ string, intent json.RawMessage) (storage.Refund, error) {
	f.intent = intent
	f.record.RequestData = json.RawMessage(`[` + string(intent) + `]`)
	return f.record, nil
}
func (f *refundStoreFake) RecordRefundPayoutSuccess(_ context.Context, _ string, id *string, response json.RawMessage) (storage.Refund, error) {
	status := storage.RefundPendingDisbursement
	f.record.Status = &status
	f.record.DisbursementID = id
	f.record.DisbursementResponse = response
	f.providerResponse = response
	return f.record, nil
}
func (f *refundStoreFake) RecordRefundPayoutFailure(_ context.Context, _ string, reason string, response json.RawMessage) error {
	status := storage.RefundFail
	f.record.Status = &status
	f.failedReason = reason
	f.providerResponse = response
	return nil
}
func (f *refundStoreFake) MarkRefundFailedIfPresent(context.Context, string, string) error {
	return storage.ErrNotFound
}
func (f *refundStoreFake) InsertRefundLog(_ context.Context, record storage.RefundLog) error {
	f.logs = append(f.logs, record)
	return nil
}

type credentialStub struct{ calls int }

func (c *credentialStub) XenditCredential(context.Context, string) (coreclient.XenditCredential, error) {
	c.calls++
	return coreclient.XenditCredential{SecretKey: "secret"}, nil
}

type encryptorStub struct{ encryptCalls, blindCalls int }

func (e *encryptorStub) BlindIndex(context.Context, string) (string, error) {
	e.blindCalls++
	return "hash", nil
}
func (e *encryptorStub) Encrypt(context.Context, string) (encryptor.Ciphertext, error) {
	e.encryptCalls++
	return encryptor.Ciphertext{Enc: "AA==", IV: "AA==", Tag: "AA==", EDK: "AA==", Alg: "AES-256-GCM", KMD: map[string]any{}}, nil
}

type signerStub struct{ payload string }

func (s *signerStub) Sign(raw []byte) (string, error) {
	s.payload = string(raw)
	return "signature", nil
}

type providerStub struct {
	store                      *refundStoreFake
	createResult, statusResult ProviderResult
	create                     PayoutRequest
	idempotency, secret        string
	createCalls, statusCalls   int
}

func (p *providerStub) Create(_ context.Context, secret, idempotency string, request PayoutRequest) ProviderResult {
	p.createCalls++
	p.secret = secret
	p.idempotency = idempotency
	p.create = request
	if p.store != nil && len(p.store.intent) == 0 {
		return ProviderResult{Status: 500, Err: errors.New("intent not persisted")}
	}
	return p.createResult
}
func (p *providerStub) Status(context.Context, string, string) ProviderResult {
	p.statusCalls++
	return p.statusResult
}

func createFixture() CreateRequest {
	var request CreateRequest
	request.ReqData.Account = Account{BankID: "BCA", AccountNo: "1234567890", Name: "Test Customer", AccountType: "saving", IDNo: "987654321", IDType: "1"}
	amount := int64(10000)
	request.ReqData.Invoice = Invoice{OrderID: "ORDER-1", RefundAmount: &amount, Reason: "Duplicate payment", Passengers: "Passenger", OriginalOrderNumber: "ORDER-0", NotifyURL: "https://ticketing.example/refund", TicketOffice: "kcic"}
	request.SignMsg = "signature"
	return request
}
func serviceFixture() (*Service, *refundStoreFake, *providerStub, *signerStub, *encryptorStub) {
	code := "ID_BCA"
	store := &refundStoreFake{bank: storage.RefundBank{ID: "bank", XenditCode: &code, Status: storage.BankEnabled}}
	provider := &providerStub{store: store, createResult: ProviderResult{Status: 200, Data: json.RawMessage(`{"id":"payout-1","amount":10000,"channel_properties":{"account_number":"1234567890"}}`)}}
	signer := &signerStub{}
	encryptor := &encryptorStub{}
	service := New(store, &credentialStub{}, encryptor, signer, provider, Config{Environment: "development", FeeFix: 2100, PPNValue: 11}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return service, store, provider, signer, encryptor
}

func TestCreatePersistsMaskedIntentBeforeXenditAndResultBeforeReply(t *testing.T) {
	service, store, provider, signer, encrypt := serviceFixture()
	response, err := service.Create(t.Context(), createFixture())
	if err != nil {
		t.Fatal(err)
	}
	if response.RetCode != 0 || response.RetMsg != "Success" || response.SignMsg != "signature" {
		t.Fatalf("response=%#v", response)
	}
	if provider.createCalls != 1 || provider.secret != "secret" || provider.idempotency != "ORDER-1" {
		t.Fatalf("provider=%#v", provider)
	}
	if provider.create.ReferenceID != "ORDER-1" || provider.create.ChannelCode != "ID_BCA" || provider.create.ChannelProperties.AccountNumber != "1234567890" || provider.create.Amount != 10000 || provider.create.IdempotencyKey != "ORDER-1" {
		t.Fatalf("payout=%#v", provider.create)
	}
	if string(store.intent) != "{\"reference_id\":\"ORDER-1\",\"channel_code\":\"ID_BCA\",\"channel_properties\":{\"account_number\":\"******7890\",\"account_holder_name\":\"Test Customer\"},\"amount\":10000,\"description\":\"Duplicate payment\",\"currency\":\"IDR\",\"idempotencyKey\":\"ORDER-1\"}" {
		t.Fatalf("intent=%s", store.intent)
	}
	if string(store.created.Data) == "" || json.Valid(store.created.Data) == false {
		t.Fatalf("stored request=%s", store.created.Data)
	}
	if string(store.created.Data) == string(mustJSON(t, createFixture())) {
		t.Fatal("plaintext request was persisted")
	}
	if string(store.created.AmountData) != "{\"amount\":10000,\"fee\":2100,\"AmountAfterFee\":12100,\"tax\":231,\"totalAmount\":12331}" {
		t.Fatalf("amount data=%s", store.created.AmountData)
	}
	if store.created.Status != storage.RefundPendingChecking || store.created.Detail == nil || store.created.BankDataCreate == nil || encrypt.encryptCalls != 1 {
		t.Fatalf("created=%#v encrypt=%d", store.created, encrypt.encryptCalls)
	}
	if store.record.Status == nil || *store.record.Status != storage.RefundPendingDisbursement || store.record.DisbursementID == nil || *store.record.DisbursementID != "payout-1" {
		t.Fatalf("record=%#v", store.record)
	}
	if string(store.providerResponse) == "" || string(store.providerResponse) == string(provider.createResult.Data) {
		t.Fatalf("provider response not masked: %s", store.providerResponse)
	}
	if signer.payload != "{\"invoice\":{\"orderId\":\"ORDER-1\",\"status\":\"PG process\"}}" {
		t.Fatalf("signed=%s", signer.payload)
	}
}

func TestCreateTicketCallZeroSkipsMockedDetail(t *testing.T) {
	service, store, _, _, _ := serviceFixture()
	request := createFixture()
	zero := int64(0)
	request.TicketCall = &zero
	if _, err := service.Create(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if store.created.Status != storage.RefundRBDApproval || store.created.Detail != nil {
		t.Fatalf("created=%#v", store.created)
	}
}

func TestCreateProviderFailurePersistsFailureAndReturnsCompatibleError(t *testing.T) {
	service, store, provider, _, _ := serviceFixture()
	provider.createResult = ProviderResult{Status: 500, Data: json.RawMessage(`{"message":"rejected","channel_properties":{"account_number":"1234567890"}}`)}
	_, err := service.Create(t.Context(), createFixture())
	var public *PublicError
	if !errors.As(err, &public) || public.HTTPStatus != 500 {
		t.Fatalf("error=%#v", err)
	}
	if store.failedReason != "Failed Xendit disbursement" || string(store.providerResponse) == "" || len(store.logs) != 1 {
		t.Fatalf("failure reason=%s response=%s logs=%#v", store.failedReason, store.providerResponse, store.logs)
	}
	if string(store.providerResponse) == string(provider.createResult.Data) {
		t.Fatal("plaintext provider failure was persisted")
	}
}

func TestDuplicateStopsBeforeSensitiveOrProviderWork(t *testing.T) {
	service, store, provider, _, encrypt := serviceFixture()
	store.exists = true
	_, err := service.Create(t.Context(), createFixture())
	var public *PublicError
	if !errors.As(err, &public) || public.HTTPStatus != 409 {
		t.Fatalf("error=%#v", err)
	}
	if provider.createCalls != 0 || encrypt.blindCalls != 0 || encrypt.encryptCalls != 0 {
		t.Fatalf("provider=%d blind=%d encrypt=%d", provider.createCalls, encrypt.blindCalls, encrypt.encryptCalls)
	}
	if len(store.logs) != 1 || store.failedReason != "" {
		t.Fatalf("logs=%#v failure=%q", store.logs, store.failedReason)
	}
}

func TestStatusUsesProviderDataMasksAccountAndSignsExactInvoice(t *testing.T) {
	service, store, provider, signer, _ := serviceFixture()
	request := createFixture()
	masked := request
	masked.ReqData.Account.AccountNo = "******7890"
	masked.ReqData.Account.IDNo = "*****4321"
	raw := mustJSON(t, masked)
	status := storage.RefundPendingDisbursement
	amount := "10000"
	ga := "ORDER-1"
	payoutID := "payout-1"
	store.record = storage.Refund{ID: "refund-1", RefundGANumber: &ga, Status: &status, Amount: &amount, Data: raw, DisbursementID: &payoutID}
	provider.statusResult = ProviderResult{Status: 200, Data: json.RawMessage(`{"amount":10000,"updated":"2026-09-29T01:02:03Z","channel_properties":{"account_number":"1234567890"}}`)}
	var query StatusRequest
	query.ReqData.Invoice.OrderID = "ORDER-1"
	query.SignMsg = "signature"
	response, err := service.Status(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	invoice := response.RetData.Invoice.(StatusInvoice)
	if invoice.BankNo == nil || *invoice.BankNo != "******7890" || invoice.TradeTime == nil || *invoice.TradeTime != "20260929080203" || invoice.Status != "PG process" {
		t.Fatalf("invoice=%#v", invoice)
	}
	if signer.payload != "{\"invoice\":{\"balance\":10000,\"bankCode\":\"BCA\",\"bankNo\":\"******7890\",\"curType\":\"360\",\"fee\":\"2100\",\"mwNo\":\"refund-1\",\"orderId\":\"ORDER-1\",\"pgCode\":\"xendit\",\"rate\":\"2100\",\"refundAmount\":\"10000\",\"status\":\"PG process\",\"tradeTime\":\"20260929080203\"}}" {
		t.Fatalf("signed=%s", signer.payload)
	}
}

func TestStatusNotFoundUsesFrozenErrorShape(t *testing.T) {
	service, _, provider, _, _ := serviceFixture()
	var request StatusRequest
	request.ReqData.Invoice.OrderID = "MISSING"
	request.SignMsg = "signature"
	_, err := service.Status(t.Context(), request)
	var public *PublicError
	if !errors.As(err, &public) || public.HTTPStatus != 404 || provider.statusCalls != 0 {
		t.Fatalf("error=%#v calls=%d", err, provider.statusCalls)
	}
	payload, _ := json.Marshal(public.Payload)
	if string(payload) != `{"message":"Refund not found","statusCode":404}` {
		t.Fatalf("payload=%s", payload)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
