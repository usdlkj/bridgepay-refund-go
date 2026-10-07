package grpcserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

func ServerTLSConfig(certFile, keyFile, clientCAFile, allowedClientName string, additionalAllowedClientNames ...string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && clientCAFile == "" {
		return nil, nil
	}
	allowedNames := append([]string{allowedClientName}, additionalAllowedClientNames...)
	for i := range allowedNames {
		allowedNames[i] = strings.TrimSpace(allowedNames[i])
	}
	if certFile == "" || keyFile == "" || clientCAFile == "" || allowedNames[0] == "" {
		return nil, errors.New("Refund gRPC mTLS requires certificate, key, client CA, and allowed client name")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load Refund gRPC server certificate: %w", err)
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read Refund gRPC client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("parse Refund gRPC client CA")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("Refund gRPC client certificate is required")
			}
			for _, name := range allowedNames {
				if name != "" && state.PeerCertificates[0].VerifyHostname(name) == nil {
					return nil
				}
			}
			if len(allowedNames) > 0 {
				return errors.New("Refund gRPC client identity is not allowed")
			}
			return errors.New("Refund gRPC client identity is not configured")
		},
	}, nil
}
