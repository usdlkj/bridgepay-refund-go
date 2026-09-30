package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var sensitiveJSONKeys = map[string]struct{}{
	"accountno": {}, "accountnumber": {}, "account_number": {},
	"bankno": {}, "banknumber": {}, "idno": {}, "identitynumber": {}, "identity_number": {},
}

// ValidateSanitizedJSON prevents generic JSON columns from becoming an escape
// hatch for plaintext account/identity data. Ciphertext belongs in the typed
// encrypted columns; sensitive values retained for display must be masked.
func ValidateSanitizedJSON(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return errors.New("invalid JSON")
	}
	return walkSensitive(value, "$")
}

func walkSensitive(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", ""), "_", ""))
			_, sensitive := sensitiveJSONKeys[strings.ToLower(key)]
			if !sensitive {
				_, sensitive = sensitiveJSONKeys[normalized]
			}
			if sensitive {
				text, ok := child.(string)
				if ok && text != "" && !strings.Contains(text, "*") {
					return fmt.Errorf("plaintext-sensitive value rejected at %s.%s", path, key)
				}
			}
			if err := walkSensitive(child, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := walkSensitive(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCiphertext(raw json.RawMessage) error {
	var payload struct {
		Enc, IV, Tag, EDK, Algorithm string
		KMD                          map[string]any
	}
	var wire map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &wire) != nil {
		return errors.New("encrypted account payload is required")
	}
	if err := json.Unmarshal(wire["enc"], &payload.Enc); err != nil || payload.Enc == "" {
		return errors.New("encrypted account payload has no ciphertext")
	}
	_ = json.Unmarshal(wire["iv"], &payload.IV)
	_ = json.Unmarshal(wire["tag"], &payload.Tag)
	_ = json.Unmarshal(wire["edk"], &payload.EDK)
	_ = json.Unmarshal(wire["alg"], &payload.Algorithm)
	_ = json.Unmarshal(wire["kmd"], &payload.KMD)
	if payload.IV == "" || payload.Tag == "" || payload.EDK == "" || payload.Algorithm != "AES-256-GCM" || payload.KMD == nil {
		return errors.New("encrypted account payload is not Encryptor-compatible")
	}
	return nil
}
