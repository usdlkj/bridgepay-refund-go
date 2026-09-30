package refund

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/storage"
)

type webhookStoreFake struct {
	insert        storage.RefundWebhookInsert
	work          storage.RefundWebhookWork
	completed     bool
	released      bool
	terminal      bool
	ticketingLogs []storage.TicketingCallLog
}

func (f *webhookStoreFake) InsertRefundWebhookCall(_ context.Context, input storage.RefundWebhookInsert) (string, error) {
	f.insert = input
	return "callback-1", nil
}
func (f *webhookStoreFake) PrepareRefundWebhook(context.Context, string, string, string, string, json.RawMessage, int, time.Duration) (storage.RefundWebhookWork, error) {
	return f.work, nil
}
func (f *webhookStoreFake) ReleaseRefundWebhook(context.Context, string) error {
	f.released = true
	return nil
}
func (f *webhookStoreFake) CompleteRefundWebhook(context.Context, string, string, json.RawMessage, int) error {
	f.completed = true
	return nil
}
func (f *webhookStoreFake) FailRefundWebhook(context.Context, string, string, string, int) error {
	f.terminal = true
	return nil
}
func (f *webhookStoreFake) InsertTicketingCallLog(_ context.Context, record storage.TicketingCallLog) error {
	f.ticketingLogs = append(f.ticketingLogs, record)
	return nil
}
func (f *webhookStoreFake) ConfigurationValue(context.Context, string) (string, error) {
	return "", storage.ErrNotFound
}

type callbackCredentialStub struct{ token string }

func (c callbackCredentialStub) XenditCredential(context.Context, string) (coreclient.XenditCredential, error) {
	return coreclient.XenditCredential{SecretKey: "secret", CallbackToken: c.token}, nil
}

type webhookPublisherStub struct {
	pattern, id string
	payload     any
}

func (p *webhookPublisherStub) PublishEvent(_ context.Context, pattern string, payload any, id string) error {
	p.pattern, p.payload, p.id = pattern, payload, id
	return nil
}

type balanceStub struct{ result ProviderResult }

func (b balanceStub) Balance(context.Context, string) ProviderResult { return b.result }

type notifierStub struct {
	endpoint string
	payload  any
	result   TicketingResult
}

func (n *notifierStub) Notify(_ context.Context, endpoint string, payload any) TicketingResult {
	n.endpoint, n.payload = endpoint, payload
	return n.result
}

func validCallbackJSON() json.RawMessage {
	return json.RawMessage(`{"event":"payout.succeeded","business_id":"business-1","created":"2026-09-29T01:00:00Z","data":{"id":"payout-1","amount":10000,"channel_code":"ID_BCA","currency":"IDR","status":"SUCCEEDED","reference_id":"ORDER-1","created":"2026-09-29T01:00:00Z","updated":"2026-09-29T02:00:00Z","channel_properties":{"account_number":"1234567890"}}}`)
}

func TestWebhookAcceptAuthenticatesSanitizesPersistsAndPublishes(t *testing.T) {
	store, publisher := &webhookStoreFake{}, &webhookPublisherStub{}
	service := NewWebhookService(store, callbackCredentialStub{token: "callback-secret"}, balanceStub{}, &notifierStub{}, &signerStub{}, publisher, "development", slog.New(slog.NewTextHandler(io.Discard, nil)))

	if _, err := service.Accept(t.Context(), "wrong", validCallbackJSON()); err == nil {
		t.Fatal("invalid token was accepted")
	}
	response, err := service.Accept(t.Context(), "callback-secret", validCallbackJSON())
	if err != nil || response["message"] != "OK" {
		t.Fatalf("response=%v error=%v", response, err)
	}
	if store.insert.RefundReference != "ORDER-1" || store.insert.DedupeKey == "" || strings.Contains(string(store.insert.Payload), "1234567890") || !strings.Contains(string(store.insert.Payload), "******7890") {
		t.Fatalf("stored callback=%+v", store.insert)
	}
	if publisher.pattern != xenditCallbackEvent || publisher.id != "refund.xendit.callback:callback-1" {
		t.Fatalf("publish=%q %q", publisher.pattern, publisher.id)
	}
}

func TestWebhookProcessNotifiesTicketingWithNodeCompatibleSignedPayload(t *testing.T) {
	status := storage.RefundSuccess
	order := "ORDER-1"
	amountData := json.RawMessage(`{"amount":10000,"fee":2100,"AmountAfterFee":12100,"tax":231,"totalAmount":12331}`)
	request := createFixture()
	request.ReqData.Account.AccountNo = "******7890"
	requestJSON, _ := json.Marshal(request)
	store := &webhookStoreFake{work: storage.RefundWebhookWork{
		State: storage.WebhookFollowUp, Outcome: "success",
		Call:   storage.RefundWebhookCall{Payload: validCallbackJSON()},
		Refund: &storage.Refund{ID: "refund-1", RefundGANumber: &order, Status: &status, AmountData: amountData, Data: requestJSON},
	}}
	notifier := &notifierStub{result: TicketingResult{Status: 200, Body: map[string]any{"retCode": json.Number("0")}, Raw: json.RawMessage(`{"retCode":0}`), StartedAt: time.Now(), FinishedAt: time.Now()}}
	signer := &signerStub{}
	service := NewWebhookService(store, callbackCredentialStub{token: "token"}, balanceStub{ProviderResult{Status: 200, Data: json.RawMessage(`{"balance":500000}`)}}, notifier, signer, &webhookPublisherStub{}, "development", slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := service.Process(t.Context(), "callback-1", 0, 3); err != nil {
		t.Fatal(err)
	}
	if !store.completed || len(store.ticketingLogs) != 1 || notifier.endpoint != request.ReqData.Invoice.NotifyURL {
		t.Fatalf("completed=%v logs=%d endpoint=%q", store.completed, len(store.ticketingLogs), notifier.endpoint)
	}
	if strings.Contains(string(store.ticketingLogs[0].Payload), "1234567890") || !strings.Contains(string(store.ticketingLogs[0].Payload), `"status":"success"`) {
		t.Fatalf("ticketing payload=%s", store.ticketingLogs[0].Payload)
	}
	wantSignInput := `{"balance":500000,"bankCode":"BCA","bankNo":"******7890","curType":"360","fee":2100,"mwNo":"refund-1","orderId":"ORDER-1","pgCode":"xendit","rate":"0%+2100","refundAmount":10000,"status":"success","tradeTime":"20260929090000"}`
	if signer.payload != wantSignInput {
		t.Fatalf("sign input=%s", signer.payload)
	}
}

func TestWebhookFollowUpRetriesThenPersistsTerminalFailure(t *testing.T) {
	status := storage.RefundSuccess
	order := "ORDER-1"
	requestJSON, _ := json.Marshal(createFixture())
	store := &webhookStoreFake{work: storage.RefundWebhookWork{State: storage.WebhookFollowUp, Outcome: "success",
		Call: storage.RefundWebhookCall{Payload: validCallbackJSON()}, Refund: &storage.Refund{ID: "refund-1", RefundGANumber: &order, Status: &status, AmountData: json.RawMessage(`{"amount":10000,"fee":2100}`), Data: requestJSON}}}
	service := NewWebhookService(store, callbackCredentialStub{token: "token"}, balanceStub{ProviderResult{Err: errors.New("balance unavailable")}}, &notifierStub{}, &signerStub{}, &webhookPublisherStub{}, "development", slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := service.Process(t.Context(), "callback-1", 0, 3); err == nil || !store.released || store.terminal {
		t.Fatalf("first failure err=%v released=%v terminal=%v", err, store.released, store.terminal)
	}
	store.released = false
	if err := service.Process(t.Context(), "callback-1", 3, 3); err == nil || !store.terminal || store.released {
		t.Fatalf("terminal failure err=%v released=%v terminal=%v", err, store.released, store.terminal)
	}
}
