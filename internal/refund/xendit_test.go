package refund

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestXenditPayoutCreateAndStatusContracts(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("secret:")) {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type=%q", r.Header.Get("Content-Type"))
		}
		switch calls {
		case 1:
			if r.Method != http.MethodPost || r.URL.Path != "/v2/payouts" {
				t.Errorf("create=%s %s", r.Method, r.URL.Path)
			}
			if r.Header.Get("Idempotency-key") != "ORDER-1" {
				t.Errorf("idempotency=%q", r.Header.Get("Idempotency-key"))
			}
			var body PayoutRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.ReferenceID != "ORDER-1" || body.ChannelCode != "ID_BCA" || body.ChannelProperties.AccountNumber != "123" || body.IdempotencyKey != "ORDER-1" {
				t.Errorf("body=%#v", body)
			}
			_, _ = w.Write([]byte(`{"id":"payout-1"}`))
		case 2:
			if r.Method != http.MethodGet || r.URL.Path != "/v2/payouts/payout-1" {
				t.Errorf("status=%s %s", r.Method, r.URL.Path)
			}
			if r.Header.Get("Idempotency-key") != "" {
				t.Error("status request included idempotency header")
			}
			_, _ = w.Write([]byte(`{"id":"payout-1","status":"SUCCEEDED"}`))
		case 3:
			if r.Method != http.MethodGet || r.URL.Path != "/balance" {
				t.Errorf("balance=%s %s", r.Method, r.URL.Path)
			}
			_, _ = w.Write([]byte(`{"balance":500000}`))
		}
	}))
	defer server.Close()
	client := NewXenditClient(server.URL)
	payload := PayoutRequest{ReferenceID: "ORDER-1", ChannelCode: "ID_BCA", ChannelProperties: PayoutChannelProperties{AccountNumber: "123", AccountHolderName: "A"}, Amount: 1000, Description: "refund", Currency: "IDR", IdempotencyKey: "ORDER-1"}
	if result := client.Create(t.Context(), "secret", "ORDER-1", payload); result.Status != 200 || result.Err != nil {
		t.Fatalf("create=%#v", result)
	}
	if result := client.Status(t.Context(), "secret", "payout-1"); result.Status != 200 || result.Err != nil {
		t.Fatalf("status=%#v", result)
	}
	if result := client.Balance(t.Context(), "secret"); result.Status != 200 || result.Err != nil || string(result.Data) != `{"balance":500000}` {
		t.Fatalf("balance=%#v", result)
	}
}

func TestXenditNon200IsReturnedForPersistence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"invalid destination"}`))
	}))
	defer server.Close()
	result := NewXenditClient(server.URL).Create(t.Context(), "secret", "ORDER", PayoutRequest{})
	if result.Status != 400 || string(result.Data) != `{"message":"invalid destination"}` || result.Err != nil {
		t.Fatalf("result=%#v", result)
	}
}
