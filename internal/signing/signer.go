// Package signing implements the Node refund service's RSA-SHA256 response
// signature contract.
package signing

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

type Signer struct{ key *rsa.PrivateKey }

func Load(path, passphrase string) (*Signer, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read signing private key: %w", err)
	}
	block, _ := pem.Decode(contents)
	if block == nil {
		return nil, errors.New("signing private key is not PEM encoded")
	}
	der := block.Bytes
	if x509.IsEncryptedPEMBlock(block) {
		if passphrase == "" {
			return nil, errors.New("signing private key requires a passphrase")
		}
		der, err = x509.DecryptPEMBlock(block, []byte(passphrase))
		if err != nil {
			return nil, errors.New("decrypt signing private key")
		}
	}
	key, err := parseRSAKey(der)
	if err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("validate signing private key: %w", err)
	}
	return &Signer{key: key}, nil
}

func parseRSAKey(der []byte) (*rsa.PrivateKey, error) {
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	value, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, errors.New("parse signing private key")
	}
	key, ok := value.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("signing private key must be RSA")
	}
	return key, nil
}

func (s *Signer) Sign(message []byte) (string, error) {
	if s == nil || s.key == nil {
		return "", errors.New("signer is unavailable")
	}
	digest := sha256.Sum256(message)
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign response: %w", err)
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}
