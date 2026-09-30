package coreclient

import (
	"context"
	"net"
	"strings"
	"testing"

	refundv1 "bridgepay-refund-go/internal/coreclient/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type coreFixture struct {
	code string
	json string
	err  error
}

type wireCoreFixture struct {
	refundv1.UnimplementedRefundCoreServiceServer
}

func (wireCoreFixture) GetPaymentGatewayCredential(_ context.Context, request *refundv1.GetPaymentGatewayCredentialRequest) (*refundv1.GetPaymentGatewayCredentialResponse, error) {
	return &refundv1.GetPaymentGatewayCredentialResponse{CredentialJson: `{"development":{"secretKey":"wire-secret"}}`}, nil
}

func (f *coreFixture) GetPaymentGatewayCredential(_ context.Context, request *refundv1.GetPaymentGatewayCredentialRequest, _ ...grpc.CallOption) (*refundv1.GetPaymentGatewayCredentialResponse, error) {
	f.code = request.GetPaymentGatewayCode()
	return &refundv1.GetPaymentGatewayCredentialResponse{CredentialJson: f.json}, f.err
}

func TestXenditCredentialSelectsNodeCompatibleEnvironment(t *testing.T) {
	fixture := &coreFixture{json: `{"development":{"secretKey":"sk_test","callbackToken":"cb_test"},"production":{"secretKey":"sk_live","callbackToken":"cb_live"}}`}
	client := &Client{core: fixture}
	development, err := client.XenditCredential(context.Background(), "staging")
	if err != nil || development.SecretKey != "sk_test" {
		t.Fatalf("development = %#v, %v", development, err)
	}
	production, err := client.XenditCredential(context.Background(), "production")
	if err != nil || production.SecretKey != "sk_live" || fixture.code != "xendit" {
		t.Fatalf("production = %#v, %v; code=%q", production, err, fixture.code)
	}
}

func TestPaymentGatewayCredentialRejectsInvalidJSON(t *testing.T) {
	client := &Client{core: &coreFixture{json: `not-json`}}
	_, err := client.PaymentGatewayCredential(context.Background(), "xendit")
	if err == nil || !strings.Contains(err.Error(), "invalid credential JSON") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientTLSConfigRequiresCompleteTriplet(t *testing.T) {
	_, err := ClientTLSConfig("ca.pem", "", "", "core")
	if err == nil {
		t.Fatal("expected incomplete mTLS configuration error")
	}
}

func TestPaymentGatewayCredentialGRPCWire(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	refundv1.RegisterRefundCoreServiceServer(server, wireCoreFixture{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := &Client{conn: connection, core: refundv1.NewRefundCoreServiceClient(connection)}
	credential, err := client.PaymentGatewayCredential(context.Background(), "xendit")
	if err != nil || !strings.Contains(credential, "wire-secret") {
		t.Fatalf("credential=%q error=%v", credential, err)
	}
}
