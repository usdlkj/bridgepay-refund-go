package grpcserver

import (
	"context"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"bridgepay-refund-go/internal/backoffice"
	backofficev1 "bridgepay-refund-go/internal/backofficepb"
	refundv1 "bridgepay-refund-go/internal/coreclient/pb"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refundbank"
	"bridgepay-refund-go/internal/storage"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const backofficeTestSecret = "synthetic-backoffice-secret-at-least-32"

type backofficeBanksFixture struct{ banksFixture }

func (backofficeBanksFixture) List(ctx context.Context, filters []storage.BankFilter) ([]refundbank.Record, error) {
	if user, ok := BackofficeUserFromContext(ctx); !ok || user.ID != "user-1" || BackofficeRequestIDFromContext(ctx) != "request-1" {
		return nil, status.Error(codes.Internal, "verified identity/request ID not forwarded")
	}
	if len(filters) != 1 || filters[0] != (storage.BankFilter{Column: 0, Value: "BCA"}) {
		return nil, status.Error(codes.Internal, "bank filters not forwarded")
	}
	return []refundbank.Record{{ID: "bank-1", BankName: "BCA"}}, nil
}

// Run after Core's Node interop test with the same TEST_BACKOFFICE_JWT_FILE
// to verify the real Backoffice helper forwards a Core-issued JWT to Refund.
func TestBackofficeNodeRefundInterop(t *testing.T) {
	if os.Getenv("TEST_BACKOFFICE_CLIENT_MODULE") == "" || os.Getenv("TEST_BACKOFFICE_JWT_FILE") == "" {
		t.Skip("TEST_BACKOFFICE_CLIENT_MODULE and TEST_BACKOFFICE_JWT_FILE are required")
	}
	token, err := os.ReadFile(os.Getenv("TEST_BACKOFFICE_JWT_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	ca, key, caPEM := testCertificateAuthority(t)
	serverCert, serverKey := testLeafCertificate(t, ca, key, "localhost", true)
	clientCert, clientKey := testLeafCertificate(t, ca, key, "backoffice.local", false)
	dir := t.TempDir()
	caFile := writePEM(t, dir, "ca.pem", caPEM)
	serverTLS, err := ServerTLSConfig(writePEM(t, dir, "server.pem", serverCert), writePEM(t, dir, "server-key.pem", serverKey), caFile, "gateway.local", "backoffice.local")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := New(serverTLS, lifecycle.New(), slog.New(slog.NewTextHandler(io.Discard, nil)), Dependencies{BackofficeJWTSecret: backofficeTestSecret, GatewayClientName: "gateway.local", BackofficeClientName: "backoffice.local", Banks: backofficeBanksFixture{}, Backoffice: backofficeRefundsFixture{}})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	app, err := filepath.Abs("../../../../02_midware-backoffice/bridgepay-backoffice")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "-e", `
const assert = require('node:assert/strict');
const {callRefund} = require(process.env.TEST_BACKOFFICE_CLIENT_MODULE);
(async () => {
  const token = process.env.TEST_BACKOFFICE_JWT;
  const query = {query:[{data:0, search:{value:'BCA'}}]};
  const {data:banks} = await callRefund('ListBanks', {query}, token, 'request-1');
  assert.equal(banks[0].bankName, 'BCA');
  const {data:refunds} = await callRefund('ListRefunds', {query}, token, 'request-1');
  assert.deepEqual(refunds, []);
  const {data:changed} = await callRefund('SetBankEnabled', {id:'bank-1', enabled:false}, token);
  assert.equal(changed.bankStatus, 'disable');
  const {data:synced} = await callRefund('SyncBanks', {}, token);
  assert.equal(synced.status, 200);
  await assert.rejects(callRefund('ListRefunds', {query}, undefined), e => e.response.status === 401);
  await assert.rejects(callRefund('ListBanks', {query:{query:'invalid'}}, token), e => e.response.status === 400);
})().then(() => process.exit(0), e => {console.error(e); process.exit(1)});
`)
	command.Dir = app
	command.Env = append(os.Environ(), "NODE_PATH="+filepath.Join(app, "node_modules"), "TEST_BACKOFFICE_JWT="+string(token), "REFUND_GRPC_ADDRESS=localhost:"+strconv.Itoa(listener.Addr().(*net.TCPAddr).Port), "BACKOFFICE_GRPC_CLIENT_CERT_FILE="+writePEM(t, dir, "client.pem", clientCert), "BACKOFFICE_GRPC_CLIENT_KEY_FILE="+writePEM(t, dir, "client-key.pem", clientKey), "BACKOFFICE_GRPC_SERVER_CA_FILE="+caFile)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Node Refund interop: %v\n%s", err, output)
	}
}

func (backofficeBanksFixture) Update(_ context.Context, id string, state *storage.BankStatus, deletedAt **time.Time) (refundbank.Record, error) {
	if id != "bank-1" {
		return refundbank.Record{}, storage.ErrNotFound
	}
	if state == nil || deletedAt != nil {
		return refundbank.Record{}, status.Error(codes.Internal, "invalid update delegation")
	}
	return refundbank.Record{ID: id, BankStatus: string(*state)}, nil
}
func (backofficeBanksFixture) Sync(context.Context) error { return nil }

type backofficeRefundsFixture struct{}

func (backofficeRefundsFixture) List(ctx context.Context, filters []storage.RefundFilter) ([]backoffice.Refund, error) {
	if user, ok := BackofficeUserFromContext(ctx); !ok || user.ID != "user-1" || BackofficeRequestIDFromContext(ctx) != "request-1" {
		return nil, status.Error(codes.Internal, "verified identity/request ID not forwarded")
	}
	if len(filters) != 1 || filters[0] != (storage.RefundFilter{Column: 0, Value: "BCA"}) {
		return nil, status.Error(codes.Internal, "refund filters not forwarded")
	}
	return []backoffice.Refund{}, nil
}

func backofficeToken(t *testing.T, method jwt.SigningMethod, secret string, claims jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(method, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestBackofficeRefundWireAndAuthorization(t *testing.T) {
	ca, key, caPEM := testCertificateAuthority(t)
	cert, private := testLeafCertificate(t, ca, key, "refund.local", true)
	dir := t.TempDir()
	serverTLS, err := ServerTLSConfig(writePEM(t, dir, "server.pem", cert), writePEM(t, dir, "key.pem", private), writePEM(t, dir, "ca.pem", caPEM), "gateway.local", "backoffice.local")
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := New(serverTLS, lifecycle.New(), slog.New(slog.NewTextHandler(io.Discard, nil)), Dependencies{
		BackofficeJWTSecret: backofficeTestSecret, GatewayClientName: "gateway.local", BackofficeClientName: "backoffice.local",
		Banks: backofficeBanksFixture{}, Backoffice: backofficeRefundsFixture{},
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	connect := func(name string) *grpc.ClientConn {
		cert, private := testLeafCertificate(t, ca, key, name, false)
		tlsConfig := clientTLSConfig(t, roots, cert, private)
		conn, err := grpc.NewClient("passthrough:///refund.local", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	client := backofficev1.NewBackofficeRefundServiceClient(connect("backoffice.local"))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	claims := jwt.MapClaims{"id": "user-1", "name": "Test Admin", "email": "admin@example.test", "role": "ADMIN", "exp": time.Now().Add(time.Hour).Unix()}
	token := backofficeToken(t, jwt.SigningMethodHS256, backofficeTestSecret, claims)
	authed := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token, "x-request-id", "request-1")
	query, _ := structpb.NewStruct(map[string]any{"query": []any{map[string]any{"data": 0, "search": map[string]any{"value": "BCA"}}}})
	listed, err := client.ListBanks(authed, &backofficev1.ListRequest{Query: query})
	if err != nil || listed.GetData().GetListValue().Values[0].GetStructValue().AsMap()["bankName"] != "BCA" {
		t.Fatalf("banks=%v err=%v", listed, err)
	}
	refunds, err := client.ListRefunds(authed, &backofficev1.ListRequest{Query: query})
	if err != nil || refunds.GetData().GetListValue() == nil || len(refunds.GetData().GetListValue().Values) != 0 {
		t.Fatalf("refunds=%v err=%v", refunds, err)
	}
	for _, enabled := range []bool{true, false} {
		changed, err := client.SetBankEnabled(authed, &backofficev1.SetBankEnabledRequest{Id: "bank-1", Enabled: enabled})
		expected := "disable"
		if enabled {
			expected = "enable"
		}
		if err != nil || changed.GetData().GetStructValue().AsMap()["bankStatus"] != expected {
			t.Fatalf("update=%v err=%v", changed, err)
		}
	}
	if _, err := client.SyncBanks(authed, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	_, err = client.SetBankEnabled(authed, &backofficev1.SetBankEnabledRequest{Id: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing bank = %v", err)
	}
	_, err = client.SetBankEnabled(authed, &backofficev1.SetBankEnabledRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing ID = %v", err)
	}
	for _, invalid := range []any{"not an array", []any{"not an object"}, []any{map[string]any{"data": 0.5}}, []any{map[string]any{"data": 3}}} {
		query, _ := structpb.NewStruct(map[string]any{"query": invalid})
		_, err := client.ListBanks(authed, &backofficev1.ListRequest{Query: query})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid filter %v: %v", invalid, err)
		}
	}
	for _, tc := range []struct{ name, token string }{
		{"missing", ""}, {"bad signature", backofficeToken(t, jwt.SigningMethodHS256, "wrong-secret", claims)},
		{"expired", backofficeToken(t, jwt.SigningMethodHS256, backofficeTestSecret, jwt.MapClaims{"id": "user-1", "role": "ADMIN", "exp": time.Now().Add(-time.Hour).Unix()})},
		{"missing expiry", backofficeToken(t, jwt.SigningMethodHS256, backofficeTestSecret, jwt.MapClaims{"id": "user-1", "role": "ADMIN"})},
		{"missing ID", backofficeToken(t, jwt.SigningMethodHS256, backofficeTestSecret, jwt.MapClaims{"role": "ADMIN", "exp": time.Now().Add(time.Hour).Unix()})},
		{"wrong algorithm", backofficeToken(t, jwt.SigningMethodHS384, backofficeTestSecret, claims)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.ListRefunds(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tc.token), &backofficev1.ListRequest{})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("JWT accepted: %v", err)
			}
		})
	}
	viewer := backofficeToken(t, jwt.SigningMethodHS256, backofficeTestSecret, jwt.MapClaims{"id": "user-1", "role": "VIEWER", "exp": time.Now().Add(time.Hour).Unix()})
	viewerCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+viewer, "x-request-id", "request-1")
	_, err = client.ListBanks(viewerCtx, &backofficev1.ListRequest{Query: query})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("VIEWER bank list = %v", err)
	}
	_, err = client.SetBankEnabled(viewerCtx, &backofficev1.SetBankEnabledRequest{Id: "bank-1", Enabled: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("VIEWER bank update = %v", err)
	}
	// Refund lists and bank synchronization retain the existing authenticated-
	// user policy; only bank listing/editing have ADMIN/SUPER_ADMIN restrictions.
	if _, err := client.ListRefunds(viewerCtx, &backofficev1.ListRequest{Query: query}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SyncBanks(viewerCtx, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	gatewayConn := connect("gateway.local")
	_, err = backofficev1.NewBackofficeRefundServiceClient(gatewayConn).ListRefunds(authed, &backofficev1.ListRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Gateway accessed Backoffice RPC: %v", err)
	}
	_, err = refundv1.NewRefundGatewayServiceClient(gatewayConn).ListBanks(ctx, &refundv1.ListBanksRequest{Context: &refundv1.RefundRequestContext{RequestId: "gateway-1"}})
	if err != nil {
		t.Fatalf("Gateway public RPC was broken: %v", err)
	}
	_, err = refundv1.NewRefundGatewayServiceClient(connect("backoffice.local")).ListBanks(ctx, &refundv1.ListBanksRequest{Context: &refundv1.RefundRequestContext{RequestId: "wrong-1"}})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Backoffice accessed Gateway RPC: %v", err)
	}
}
