package grpcserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServerTLSConfigRequiresAndChecksGatewayIdentity(t *testing.T) {
	dir := t.TempDir()
	ca, caKey, caPEM := testCertificateAuthority(t)
	serverCert, serverKey := testLeafCertificate(t, ca, caKey, "refund.local", true)
	allowedCert, allowedKey := testLeafCertificate(t, ca, caKey, "gateway.local", false)
	wrongCert, wrongKey := testLeafCertificate(t, ca, caKey, "other.local", false)
	caFile := writePEM(t, dir, "ca.pem", caPEM)
	serverCertFile := writePEM(t, dir, "server.pem", serverCert)
	serverKeyFile := writePEM(t, dir, "server-key.pem", serverKey)

	serverConfig, err := ServerTLSConfig(serverCertFile, serverKeyFile, caFile, "gateway.local")
	if err != nil {
		t.Fatal(err)
	}
	if serverConfig.ClientAuth != tls.RequireAndVerifyClientCert || serverConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("unexpected TLS policy: auth=%v minimum=%x", serverConfig.ClientAuth, serverConfig.MinVersion)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append test CA")
	}
	if err := handshake(serverConfig, clientTLSConfig(t, roots, allowedCert, allowedKey)); err != nil {
		t.Fatalf("allowed Gateway identity was rejected: %v", err)
	}
	if err := handshake(serverConfig, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "refund.local"}); err == nil {
		t.Fatal("unexpectedly accepted a client without a certificate")
	}
	if err := handshake(serverConfig, clientTLSConfig(t, roots, wrongCert, wrongKey)); err == nil {
		t.Fatal("unexpectedly accepted a client certificate with the wrong DNS identity")
	}
	otherCA, otherCAKey, _ := testCertificateAuthority(t)
	untrustedCert, untrustedKey := testLeafCertificate(t, otherCA, otherCAKey, "gateway.local", false)
	if err := handshake(serverConfig, clientTLSConfig(t, roots, untrustedCert, untrustedKey)); err == nil {
		t.Fatal("unexpectedly accepted a client certificate from an untrusted CA")
	}
}

func TestServerTLSConfigRejectsPartialConfiguration(t *testing.T) {
	if _, err := ServerTLSConfig("cert.pem", "", "", "gateway.local"); err == nil {
		t.Fatal("expected incomplete mTLS configuration error")
	}
}

func handshake(serverConfig, clientConfig *tls.Config) error {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	serverTLS, clientTLS := tls.Server(serverConn, serverConfig), tls.Client(clientConn, clientConfig)
	serverResult := make(chan error, 1)
	go func() { serverResult <- serverTLS.Handshake() }()
	clientErr := clientTLS.Handshake()
	_ = clientConn.Close()
	serverErr := <-serverResult
	if serverErr != nil {
		return serverErr
	}
	return clientErr
}

func clientTLSConfig(t *testing.T, roots *x509.CertPool, certificatePEM, keyPEM []byte) *tls.Config {
	t.Helper()
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{certificate}, ServerName: "refund.local"}
}

func testCertificateAuthority(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func testLeafCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, dnsName string, server bool) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.ExtKeyUsageClientAuth
	if server {
		usage = x509.ExtKeyUsageServerAuth
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: dnsName}, DNSNames: []string{dnsName}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func writePEM(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
