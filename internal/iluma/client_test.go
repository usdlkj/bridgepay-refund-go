package iluma

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientPreservesProviderContract(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.Header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("token:")) {
			t.Errorf("authorization = %q", got)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type missing")
		}
		switch calls {
		case 1:
			if r.Method != http.MethodPost || r.URL.Path != "/v1.2/identity/bank_account_validation_details" {
				t.Errorf("first request = %s %s", r.Method, r.URL.Path)
			}
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["bank_code"] != "BCA" || body["bank_account_number"] != "1234" {
				t.Errorf("body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"id":"req-1","status":"pending"}`))
		case 2:
			if r.Method != http.MethodGet || r.URL.Path != "/v1.2/identity/bank_account_validation_details/req-1" {
				t.Errorf("second request = %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"slow down"}`))
		}
	}))
	defer server.Close()
	client := NewClient(server.URL, "token", time.Second)
	if result := client.Validate(t.Context(), "BCA", "1234"); result.Status != 200 || result.Error != nil {
		t.Fatalf("validate = %#v", result)
	}
	result := client.Result(t.Context(), "req-1")
	if result.Error == nil || result.Error.Code != "ILUMA_RATE_LIMIT" || result.Error.Message != "slow down" {
		t.Fatalf("result = %#v", result)
	}
}

func TestBankListUsesFrozenURLAndLegacyFailureEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bank/available_bank_codes" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"denied"}`))
	}))
	defer server.Close()
	result := NewClient("https://unused.invalid", "token", time.Second, WithBankListURL(server.URL+"/bank/available_bank_codes")).BankList(t.Context())
	if result.Status != 500 {
		t.Fatalf("status = %d", result.Status)
	}
	if string(result.LegacyBankListEnvelope()) != `{"msg":"denied","status":500}` {
		t.Fatalf("envelope = %s", result.LegacyBankListEnvelope())
	}
}
