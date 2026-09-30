package storage

import (
	"encoding/json"
	"testing"
)

func TestValidateSanitizedJSON(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"masked", `{"accountNo":"******0001","idNo":"********1234"}`, false},
		{"plain account", `{"account":{"accountNo":"1234567890"}}`, true},
		{"plain identity", `{"identity_number":"1234567890123456"}`, true},
		{"ordinary data", `{"orderId":"RF-1","amount":1000}`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSanitizedJSON(json.RawMessage(test.value))
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateCiphertext(t *testing.T) {
	valid := json.RawMessage(`{"enc":"AA==","iv":"AA==","tag":"AA==","edk":"AA==","alg":"AES-256-GCM","kmd":{}}`)
	if err := validateCiphertext(valid); err != nil {
		t.Fatalf("valid ciphertext rejected: %v", err)
	}
	if err := validateCiphertext(json.RawMessage(`{"enc":"plaintext"}`)); err == nil {
		t.Fatal("incomplete ciphertext accepted")
	}
}
