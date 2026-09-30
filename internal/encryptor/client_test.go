package encryptor

import (
	"encoding/base64"
	"testing"
)

func TestAccountEncryptionRequestPreservesNodeContract(t *testing.T) {
	request := encryptRequest("0000000001")
	if request["value"] != base64.StdEncoding.EncodeToString([]byte("0000000001")) {
		t.Fatal("plaintext was not base64 encoded")
	}
	if request["aad"] != base64.StdEncoding.EncodeToString([]byte(AccountNumberContext)) {
		t.Fatal("AAD differs from Node contract")
	}
	if request["context"] != AccountNumberContext {
		t.Fatal("context differs from Node contract")
	}
}

func TestDecryptUsesSameAADAndContext(t *testing.T) {
	payload := Ciphertext{Enc: "AA==", IV: "AA==", Tag: "AA==", EDK: "AA==", Alg: "AES-256-GCM", KMD: map[string]any{}}
	request := decryptRequest(payload)
	if request["context"] != AccountNumberContext {
		t.Fatal("decrypt context differs from encrypt context")
	}
	if request["aad"] != base64.StdEncoding.EncodeToString([]byte(AccountNumberContext)) {
		t.Fatal("decrypt AAD differs from encrypt AAD")
	}
}
