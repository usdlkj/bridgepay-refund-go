// Package coreclient provides the typed synchronous Refund-Go to Core-Go
// boundary. Asynchronous work remains on RabbitMQ.
package coreclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	refundv1 "bridgepay-refund-go/internal/coreclient/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	core refundv1.RefundCoreServiceClient
}

type XenditCredential struct {
	SecretKey     string `json:"secretKey"`
	CallbackToken string `json:"callbackToken"`
}

type credentialEnvelope struct {
	Development json.RawMessage `json:"development"`
	Production  json.RawMessage `json:"production"`
}

func ClientTLSConfig(caFile, certFile, keyFile, serverName string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, errors.New("Core gRPC mTLS requires CA, certificate, and key files")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load Core gRPC client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read Core gRPC CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("parse Core gRPC CA")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{certificate}, ServerName: serverName}, nil
}

func Dial(target string, tlsConfig *tls.Config) (*Client, error) {
	transport := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsConfig != nil {
		transport = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target, transport)
	if err != nil {
		return nil, fmt.Errorf("dial Core gRPC: %w", err)
	}
	return &Client{conn: conn, core: refundv1.NewRefundCoreServiceClient(conn)}, nil
}

func (c *Client) PaymentGatewayCredential(ctx context.Context, code string) (string, error) {
	if c == nil || c.core == nil {
		return "", errors.New("Core gRPC client is unavailable")
	}
	code = strings.TrimSpace(code)
	if code == "" {
		return "", errors.New("payment gateway code is required")
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := c.core.GetPaymentGatewayCredential(callCtx, &refundv1.GetPaymentGatewayCredentialRequest{PaymentGatewayCode: code})
	if err != nil {
		return "", err
	}
	if !json.Valid([]byte(response.GetCredentialJson())) {
		return "", errors.New("Core returned invalid credential JSON")
	}
	return response.GetCredentialJson(), nil
}

// XenditCredential preserves Node's environment rule: production selects the
// production branch; every other environment selects development.
func (c *Client) XenditCredential(ctx context.Context, environment string) (XenditCredential, error) {
	raw, err := c.PaymentGatewayCredential(ctx, "xendit")
	if err != nil {
		return XenditCredential{}, err
	}
	var envelope credentialEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return XenditCredential{}, errors.New("decode Xendit credential envelope")
	}
	selected := envelope.Development
	if environment == "production" {
		selected = envelope.Production
	}
	var credential XenditCredential
	if len(selected) == 0 || json.Unmarshal(selected, &credential) != nil || credential.SecretKey == "" {
		return XenditCredential{}, errors.New("Xendit credential is not configured for environment")
	}
	return credential, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
