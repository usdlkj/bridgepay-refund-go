package iluma

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/storage"
)

type fakeStore struct {
	bank            storage.RefundBank
	data            storage.BankData
	logs            []storage.IlumaCallLog
	callbackUpdated bool
}

func (f *fakeStore) GetRefundBankByXenditCode(context.Context, string) (storage.RefundBank, error) {
	return f.bank, nil
}
func (f *fakeStore) FindBankData(context.Context, string, string) (storage.BankData, error) {
	if f.data.ID == "" {
		return storage.BankData{}, storage.ErrNotFound
	}
	return f.data, nil
}
func (f *fakeStore) GetOrCreateBankData(_ context.Context, in storage.BankDataInsert) (storage.BankData, bool, error) {
	result := in.Result
	f.data = storage.BankData{ID: "bank-data-1", BankCode: in.BankCode, AccountNumberHash: in.AccountNumberHash, AccountStatus: in.Status, AccountResult: &result, LastCheckAt: &in.LastCheckAt}
	return f.data, true, nil
}
func (f *fakeStore) GetBankDataByID(context.Context, string) (storage.BankData, error) {
	return f.data, nil
}
func (f *fakeStore) FindBankDataByRequestID(_ context.Context, id string) (storage.BankData, error) {
	if f.data.RequestID == nil || *f.data.RequestID != id {
		return storage.BankData{}, storage.ErrNotFound
	}
	return f.data, nil
}
func (f *fakeStore) SetBankDataRequestID(_ context.Context, _ string, id string) error {
	f.data.RequestID = &id
	return nil
}
func (f *fakeStore) CompleteBankData(_ context.Context, _ string, result storage.AccountResult, raw json.RawMessage, code, message *string) (bool, error) {
	if f.data.AccountStatus == storage.AccountExpired {
		return false, nil
	}
	f.data.AccountStatus = storage.AccountCompleted
	f.data.AccountResult = &result
	f.data.IlumaData = raw
	f.data.FailureCode = code
	f.data.FailureMessage = message
	return true, nil
}
func (f *fakeStore) ExpireBankData(context.Context, string) (bool, error) {
	if f.data.AccountStatus != storage.AccountPending {
		return false, nil
	}
	result := storage.AccountResultFailed
	f.data.AccountStatus = storage.AccountExpired
	f.data.AccountResult = &result
	return true, nil
}
func (f *fakeStore) InsertIlumaCallLog(_ context.Context, log storage.IlumaCallLog) error {
	f.logs = append(f.logs, log)
	return nil
}
func (f *fakeStore) UpdateIlumaCallback(context.Context, string, json.RawMessage, time.Time) (bool, error) {
	f.callbackUpdated = true
	return true, nil
}

type fakeEncryptor struct{ blindCalls, encryptCalls int }

func (f *fakeEncryptor) BlindIndex(context.Context, string) (string, error) {
	f.blindCalls++
	return "blind", nil
}
func (f *fakeEncryptor) Encrypt(context.Context, string) (encryptor.Ciphertext, error) {
	f.encryptCalls++
	return encryptor.Ciphertext{Enc: "e", IV: "i", Tag: "t", EDK: "k", Alg: "AES-256-GCM", KMD: map[string]any{}}, nil
}

type fakeSigner struct{ payload string }

func (f *fakeSigner) Sign(value []byte) (string, error) {
	f.payload = string(value)
	return "signature", nil
}

type fakePublisher struct {
	pattern, id string
	data        any
}

func (f *fakePublisher) PublishEvent(_ context.Context, pattern string, data any, id string) error {
	f.pattern = pattern
	f.data = data
	f.id = id
	return nil
}

type fakeProvider struct {
	validate                   HTTPResult
	validateCalls, resultCalls int
}

func (f *fakeProvider) Validate(context.Context, string, string) HTTPResult {
	f.validateCalls++
	return f.validate
}
func (f *fakeProvider) Result(context.Context, string) HTTPResult {
	f.resultCalls++
	return HTTPResult{Status: 200, Data: json.RawMessage(`{"id":"req","status":"pending"}`)}
}

func serviceFixture(status storage.AccountStatus) (*Service, *fakeStore, *fakeProvider, *fakeSigner, *fakePublisher) {
	code := "BCA"
	ilumaCode := "014"
	result := storage.AccountResultSuccess
	now := time.Now()
	store := &fakeStore{bank: storage.RefundBank{ID: "bank", BankName: "BCA", XenditCode: &code, IlumaCode: &ilumaCode, Status: storage.BankEnabled}, data: storage.BankData{ID: "bank-data-1", BankCode: "BCA", AccountNumberHash: "blind", AccountStatus: status, AccountResult: &result, LastCheckAt: &now}}
	provider := &fakeProvider{validate: HTTPResult{Status: 200, Data: json.RawMessage(`{"id":"req","status":"completed","result":{"is_found":true,"is_virtual_account":false,"account_holder_name":"A"}}`)}}
	signer := &fakeSigner{}
	publisher := &fakePublisher{}
	service := NewService(store, &fakeEncryptor{}, signer, publisher, provider, Config{BaseURL: "https://api.iluma.ai", TTLDays: 10, MaxWait: time.Millisecond, MinSleep: 2 * time.Millisecond, MaxSleep: 2 * time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return service, store, provider, signer, publisher
}

func requestFixture() CheckAccountRequest {
	var request CheckAccountRequest
	request.SignMsg = "signed"
	request.ReqData.Account = Account{BankID: "BCA", AccountNo: "12345678", AccountType: "saving", IDNo: "id", IDType: "ktp", Name: "A"}
	return request
}

func TestCheckAccountRevalidatesFreshCompletedRowAndSignsExactStatus(t *testing.T) {
	service, store, provider, signer, _ := serviceFixture(storage.AccountCompleted)
	response, err := service.CheckAccount(t.Context(), requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	if provider.validateCalls != 1 {
		t.Fatalf("validate calls = %d", provider.validateCalls)
	}
	if response.RetCode != 0 || response.RetData == nil || response.RetData.Status != "success" || response.SignMsg != "signature" {
		t.Fatalf("response = %#v", response)
	}
	if signer.payload != `{"status":"success"}` {
		t.Fatalf("signed payload = %s", signer.payload)
	}
	if len(store.logs) != 1 || string(store.logs[0].Payload) != `{"bank_account_number":"****5678","bank_code":"BCA"}` {
		t.Fatalf("logs = %#v", store.logs)
	}
}

func TestTimeoutExpiresThenPublishesFrozenFallbackEvent(t *testing.T) {
	service, store, provider, _, publisher := serviceFixture(storage.AccountPending)
	provider.validate = HTTPResult{Status: 200, Data: json.RawMessage(`{"id":"req-timeout","status":"pending"}`)}
	response, err := service.CheckAccount(t.Context(), requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	if response.RetData == nil || response.RetData.Status != "failed" {
		t.Fatalf("response = %#v", response)
	}
	if store.data.AccountStatus != storage.AccountExpired {
		t.Fatalf("status = %s", store.data.AccountStatus)
	}
	if publisher.pattern != "refund.iluma.poll" || publisher.id != "refund.iluma.poll:req-timeout:bank-data-1" {
		t.Fatalf("published = %#v", publisher)
	}
	before := provider.resultCalls
	if err := service.Poll(t.Context(), PollRequest{RequestID: "req-timeout", BankDataID: "bank-data-1"}); err != nil {
		t.Fatal(err)
	}
	if provider.resultCalls != before {
		t.Fatal("expired fallback must exit without provider call to preserve frozen behavior")
	}
}

func TestCallbackAcknowledgesUnknownAndCompletesPendingWithoutRetrySpam(t *testing.T) {
	service, store, _, _, _ := serviceFixture(storage.AccountPending)
	unknown, err := service.Callback(t.Context(), json.RawMessage(`{"id":"unknown","status":"completed"}`))
	if err != nil || unknown["message"] != "Ignored" {
		t.Fatalf("unknown = %#v, %v", unknown, err)
	}
	id := "req"
	store.data.RequestID = &id
	ack, err := service.Callback(t.Context(), json.RawMessage(`{"id":"req","status":"completed","result":{"is_found":false,"is_virtual_account":false,"error_code":"NOT_FOUND","message":"missing"}}`))
	if err != nil || ack["message"] != "OK" {
		t.Fatalf("ack = %#v, %v", ack, err)
	}
	if store.data.AccountStatus != storage.AccountCompleted || *store.data.AccountResult != storage.AccountResultFailed {
		t.Fatalf("data = %#v", store.data)
	}
	if store.data.FailureCode == nil || *store.data.FailureCode != "NOT_FOUND" {
		t.Fatalf("failure code = %#v", store.data.FailureCode)
	}
	if !store.callbackUpdated {
		t.Fatal("callback audit row was not updated")
	}
	ack, err = service.Callback(t.Context(), json.RawMessage(`not-json`))
	if err != nil || ack["message"] != "OK" {
		t.Fatalf("invalid callback = %#v, %v", ack, err)
	}
}

func TestDisabledBankIsRejectedBeforeSensitiveProcessing(t *testing.T) {
	service, store, provider, _, _ := serviceFixture(storage.AccountPending)
	store.bank.Status = storage.BankDisabled
	response, err := service.CheckAccount(t.Context(), requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	if response.RetCode != -1 || response.RetMsg != "Bank code not found" || provider.validateCalls != 0 {
		t.Fatalf("response=%#v calls=%d", response, provider.validateCalls)
	}
}
