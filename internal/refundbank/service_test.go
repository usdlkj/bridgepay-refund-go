package refundbank

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/storage"
)

type bankStoreFake struct {
	banks        []storage.RefundBank
	log          storage.IlumaCallLog
	upserts      []string
	ilumaUpdates []string
}

func (f *bankStoreFake) ListEnabledRefundBanks(context.Context) ([]storage.RefundBank, error) {
	return f.banks, nil
}
func (f *bankStoreFake) ListRefundBanks(context.Context, []storage.BankFilter) ([]storage.RefundBank, error) {
	return f.banks, nil
}
func (f *bankStoreFake) GetRefundBankByID(context.Context, string) (storage.RefundBank, error) {
	return f.banks[0], nil
}
func (f *bankStoreFake) UpdateRefundBank(_ context.Context, _ string, status *storage.BankStatus, _ **time.Time) (storage.RefundBank, error) {
	value := f.banks[0]
	if status != nil {
		value.Status = *status
	}
	return value, nil
}
func (f *bankStoreFake) UpsertXenditRefundBank(_ context.Context, name, code string, _ json.RawMessage) (storage.RefundBank, error) {
	f.upserts = append(f.upserts, code+":"+name)
	return storage.RefundBank{}, nil
}
func (f *bankStoreFake) UpdateIlumaRefundBankByName(_ context.Context, name, code string, _ json.RawMessage) (bool, error) {
	f.ilumaUpdates = append(f.ilumaUpdates, name+":"+code)
	return name == "Bank Central Asia", nil
}
func (f *bankStoreFake) InsertIlumaCallLog(_ context.Context, record storage.IlumaCallLog) error {
	f.log = record
	return nil
}

type credentialFake struct{ environment string }

func (f *credentialFake) XenditCredential(_ context.Context, env string) (coreclient.XenditCredential, error) {
	f.environment = env
	return coreclient.XenditCredential{SecretKey: "secret"}, nil
}

type xenditFake struct{ secret string }

func (f *xenditFake) PayoutChannels(_ context.Context, secret string) ([]Channel, error) {
	f.secret = secret
	return []Channel{{Code: "BCA", Name: "Bank Central Asia", Data: json.RawMessage(`{"channelCode":"BCA"}`)}}, nil
}

type ilumaBanksFake struct{}

func (ilumaBanksFake) BankList(context.Context) iluma.HTTPResult {
	return iluma.HTTPResult{Status: 200, Data: json.RawMessage(`[{"name":"Bank Central Asia","code":"014"},{"name":"Missing Bank","code":"000"}]`)}
}

func TestSyncPreservesXenditThenIlumaContractAndAuditLog(t *testing.T) {
	store := &bankStoreFake{}
	credentials := &credentialFake{}
	xendit := &xenditFake{}
	service := New(store, credentials, xendit, ilumaBanksFake{}, "development", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := service.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if credentials.environment != "development" || xendit.secret != "secret" {
		t.Fatalf("credential path = %q %q", credentials.environment, xendit.secret)
	}
	if len(store.upserts) != 1 || store.upserts[0] != "BCA:Bank Central Asia" {
		t.Fatalf("upserts = %#v", store.upserts)
	}
	if len(store.ilumaUpdates) != 2 || store.ilumaUpdates[0] != "Bank Central Asia:014" {
		t.Fatalf("Iluma updates = %#v", store.ilumaUpdates)
	}
	if store.log.URL != "https://api.iluma.ai/bank/available_bank_codes" || store.log.Method != "get" || store.log.Function != "yggdrasilService.refund" {
		t.Fatalf("audit log = %#v", store.log)
	}
	if string(store.log.Response) != "{\"data\":[{\"code\":\"014\",\"name\":\"Bank Central Asia\"},{\"code\":\"000\",\"name\":\"Missing Bank\"}],\"status\":200}" {
		t.Fatalf("audit response = %s", store.log.Response)
	}
}

func TestPublicListKeepsLegacyEnvelope(t *testing.T) {
	code := "BCA"
	ilumaCode := "014"
	store := &bankStoreFake{banks: []storage.RefundBank{{ID: "1", BankName: "Bank Central Asia", XenditCode: &code, IlumaCode: &ilumaCode, Status: storage.BankEnabled}}}
	response, err := New(store, nil, nil, nil, "development", nil).PublicList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if response.RetCode != 0 || response.RetMsg != "success" || len(response.RetData) != 1 || *response.RetData[0].Code != "BCA" {
		t.Fatalf("response = %#v", response)
	}
}
