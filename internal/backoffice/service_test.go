package backoffice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/storage"
)

type retryStore struct {
	record   storage.Refund
	bank     storage.BankData
	intent   json.RawMessage
	response json.RawMessage
	complete bool
	failed   bool
}

func (*retryStore) ListRefunds(context.Context, []storage.RefundFilter) ([]storage.RefundAdminRecord, error) {
	return nil, nil
}
func (*retryStore) GetRefundAdmin(context.Context, string, bool) (storage.RefundAdminRecord, error) {
	return storage.RefundAdminRecord{}, storage.ErrNotFound
}
func (*retryStore) ListRefundLogs(context.Context, []storage.RefundLogFilter) ([]storage.RefundLog, error) {
	return nil, nil
}
func (*retryStore) ConfigurationInt(context.Context, string, int) (int, error) { return 2, nil }
func (s *retryStore) GetRefundByGANumber(context.Context, string) (storage.Refund, error) {
	return s.record, nil
}
func (s *retryStore) GetBankDataByID(context.Context, string) (storage.BankData, error) {
	return s.bank, nil
}
func (s *retryStore) ReserveRefundRetry(_ context.Context, _ string, _, _ int, _ time.Time, intent json.RawMessage) (storage.Refund, error) {
	s.intent = intent
	return s.record, nil
}
func (s *retryStore) CompleteRefundRetry(_ context.Context, _ string, _ *string, response json.RawMessage) error {
	s.complete = true
	s.response = response
	return nil
}
func (s *retryStore) FailRefundRetry(context.Context, string, string, json.RawMessage) error {
	s.failed = true
	return nil
}

type retryCredentials struct{}

func (retryCredentials) XenditCredential(context.Context, string) (coreclient.XenditCredential, error) {
	return coreclient.XenditCredential{SecretKey: "secret"}, nil
}

type retryDecryptor struct{}

func (retryDecryptor) Decrypt(context.Context, encryptor.Ciphertext) (string, error) {
	return "1234567890", nil
}

type retryProvider struct{ request refund.PayoutRequest }

func (p *retryProvider) Create(_ context.Context, _, _ string, request refund.PayoutRequest) refund.ProviderResult {
	p.request = request
	return refund.ProviderResult{Status: 200, Data: json.RawMessage(`{"id":"po-1","channel_properties":{"account_number":"1234567890"}}`)}
}
func (*retryProvider) Status(context.Context, string, string) refund.ProviderResult {
	return refund.ProviderResult{}
}

func TestRetryDecryptsOnlyForProviderAndPersistsMaskedIntent(t *testing.T) {
	status := storage.RefundFail
	ga := "GA-1"
	bankID := "bank-data-1"
	store := &retryStore{record: storage.Refund{ID: "refund-1", RefundGANumber: &ga, Status: &status,
		BankDataID: &bankID, RetryAttempt: json.RawMessage(`[]`),
		Data:       json.RawMessage(`{"reqData":{"account":{"name":"Customer"},"invoice":{"reason":"cancel"}}}`),
		BankData:   json.RawMessage(`{"bankCode":"ID_BCA","bankNumber":"******7890","bankName":"Customer"}`),
		AmountData: json.RawMessage(`{"amount":10000}`)},
		bank: storage.BankData{AccountNumberEnc: json.RawMessage(`{"enc":"e","iv":"i","tag":"t","edk":"d","alg":"AES-256-GCM","kmd":{}}`)}}
	provider := &retryProvider{}
	service := New(store, retryCredentials{}, retryDecryptor{}, provider, "test")
	if err := service.Retry(context.Background(), ga); err != nil {
		t.Fatal(err)
	}
	if provider.request.ReferenceID != "GA-1-1" || provider.request.IdempotencyKey != "GA-1" || provider.request.ChannelProperties.AccountNumber != "1234567890" {
		t.Fatalf("provider request = %+v", provider.request)
	}
	if string(store.intent) == "" || string(store.intent) == string(provider.request.ChannelProperties.AccountNumber) {
		t.Fatalf("intent = %s", store.intent)
	}
	if string(store.intent) != `{"reference_id":"GA-1-1","channel_code":"ID_BCA","channel_properties":{"account_number":"******7890","account_holder_name":"Customer"},"amount":10000,"description":"cancel","currency":"IDR","idempotencyKey":"GA-1"}` {
		t.Fatalf("masked intent = %s", store.intent)
	}
	if !store.complete || store.failed {
		t.Fatalf("complete=%v failed=%v", store.complete, store.failed)
	}
	if string(store.response) != `{"channel_properties":{"account_number":"******7890"},"id":"po-1"}` {
		t.Fatalf("masked response = %s", store.response)
	}
}
