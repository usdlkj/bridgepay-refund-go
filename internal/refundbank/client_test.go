package refundbank

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestXenditPayoutChannelsContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/payouts_channels" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("currency") != "IDR" || r.URL.Query().Get("channel_category") != "BANK" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("secret:")) {
			t.Errorf("bad authorization")
		}
		_, _ = w.Write([]byte(`[{"channel_code":"BCA","channel_name":"Bank Central Asia","minimum_amount":10000}]`))
	}))
	defer server.Close()
	channels, err := NewXenditClient(server.URL).PayoutChannels(t.Context(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 || channels[0].Code != "BCA" || channels[0].Name != "Bank Central Asia" {
		t.Fatalf("channels = %#v", channels)
	}
	var stored map[string]any
	if err := json.Unmarshal(channels[0].Data, &stored); err != nil {
		t.Fatal(err)
	}
	if _, ok := stored["minimumAmount"]; !ok {
		t.Fatalf("stored data was not converted to SDK-compatible camelCase: %s", channels[0].Data)
	}
}
