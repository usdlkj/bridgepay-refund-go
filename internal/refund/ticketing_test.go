package refund

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTicketingNotificationContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request=%s content-type=%q", r.Method, r.Header.Get("Content-Type"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["signMsg"] != "signature" {
			t.Errorf("body=%v error=%v", body, err)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"retCode":0}`))
	}))
	defer server.Close()
	result := NewTicketingClient().Notify(t.Context(), server.URL, map[string]any{"retData": map[string]string{"orderId": "ORDER-1"}, "signMsg": "signature"})
	if result.Err != nil || result.Status != http.StatusCreated || !result.Accepted() {
		t.Fatalf("result=%+v", result)
	}
}
