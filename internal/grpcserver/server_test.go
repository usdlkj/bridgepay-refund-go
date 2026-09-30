package grpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	refundv1 "bridgepay-refund-go/internal/coreclient/pb"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/refundbank"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type banksFixture struct{}

func (banksFixture) PublicList(context.Context) (refundbank.PublicListResponse, error) {
	code := "ID_BCA"
	return refundbank.PublicListResponse{RetCode: 0, RetMsg: "success", RetData: []refundbank.PublicBank{{Code: &code, Name: "BCA"}}}, nil
}

type ilumaFixture struct{}

func (ilumaFixture) CheckAccount(context.Context, iluma.CheckAccountRequest) (iluma.CheckAccountResponse, error) {
	return iluma.CheckAccountResponse{RetCode: 0, Message: "Success", RetData: &iluma.CheckAccountData{Status: "success"}, SignMsg: "signed"}, nil
}
func (ilumaFixture) Callback(context.Context, json.RawMessage) (map[string]string, error) {
	return map[string]string{"message": "OK"}, nil
}

type refundFixture struct{ createCalls int }

func (f *refundFixture) Create(_ context.Context, request refund.CreateRequest) (refund.Response, error) {
	f.createCalls++
	return refund.Response{RetCode: 0, RetMsg: "Success", RetData: refund.ResponseData{Invoice: refund.CreateInvoice{OrderID: request.ReqData.Invoice.OrderID, Status: "PG process"}}, SignMsg: "signed"}, nil
}
func (f *refundFixture) Status(context.Context, refund.StatusRequest) (refund.Response, error) {
	return refund.Response{RetCode: 0, RetMsg: "success", SignMsg: "signed"}, nil
}

type webhookFixture struct{ err error }

func (f webhookFixture) Accept(context.Context, string, json.RawMessage) (map[string]string, error) {
	return map[string]string{"message": "OK"}, f.err
}

func testClient(t *testing.T, tracker *lifecycle.Tracker, webhook webhookFixture) refundv1.RefundGatewayServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := New(nil, tracker, slog.New(slog.NewTextHandler(io.Discard, nil)), Dependencies{
		Banks: banksFixture{}, Iluma: ilumaFixture{}, Refund: &refundFixture{}, Webhook: webhook,
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return refundv1.NewRefundGatewayServiceClient(connection)
}

func TestRefundGatewayGRPCWireContract(t *testing.T) {
	client := testClient(t, lifecycle.New(), webhookFixture{})
	ctx := &refundv1.RefundRequestContext{RequestId: "request-1", IdempotencyKey: "ORDER-1"}
	banks, err := client.ListBanks(t.Context(), &refundv1.ListBanksRequest{Context: ctx})
	if err != nil || string(banks.GetBodyJson()) != `{"retCode":0,"retData":[{"code":"ID_BCA","name":"BCA"}],"retMsg":"success"}` {
		t.Fatalf("ListBanks() response=%s err=%v", banks.GetBodyJson(), err)
	}
	amount := int64(10000)
	created, err := client.CreateRefund(t.Context(), &refundv1.CreateRefundRequest{
		Context: ctx, SignMsg: "dGVzdA==", Account: &refundv1.RefundAccount{BankId: "BCA", AccountNo: "123456", AccountType: "saving", IdNo: "123", IdType: "1", Name: "Test Customer"},
		Invoice: &refundv1.RefundInvoice{OrderId: "ORDER-1", RefundAmount: &amount, Reason: "Duplicate payment", Passengers: "Passenger", OriginalOrderNumber: "ORDER-1", NotifyUrl: "https://merchant.example/refund", TicketOffice: "kcic"},
	})
	if err != nil || !json.Valid(created.GetBodyJson()) {
		t.Fatalf("CreateRefund() response=%s err=%v", created.GetBodyJson(), err)
	}
}

func TestRefundGatewayGRPCValidatesAndDrains(t *testing.T) {
	tracker := lifecycle.New()
	client := testClient(t, tracker, webhookFixture{})
	_, err := client.CreateRefund(t.Context(), &refundv1.CreateRefundRequest{Context: &refundv1.RefundRequestContext{RequestId: "request-1"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid request code=%s err=%v", status.Code(err), err)
	}
	tracker.BeginDrain()
	_, err = client.ListBanks(t.Context(), &refundv1.ListBanksRequest{Context: &refundv1.RefundRequestContext{RequestId: "request-2"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("draining code=%s err=%v", status.Code(err), err)
	}
}

func TestRefundGatewayGRPCMapsCallbackAuthentication(t *testing.T) {
	public := &refund.WebhookError{HTTPStatus: 401, Cause: errors.New("bad token")}
	client := testClient(t, lifecycle.New(), webhookFixture{err: public})
	_, err := client.AcceptXenditCallback(t.Context(), &refundv1.AcceptXenditCallbackRequest{
		Context: &refundv1.RefundRequestContext{RequestId: "request-1"}, PayloadJson: []byte(`{"event":"test"}`), CallbackToken: "bad",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code=%s err=%v", status.Code(err), err)
	}
}

func TestDeadlineInterceptorAppliesMethodBudget(t *testing.T) {
	interceptor := deadlineInterceptor()
	_, err := interceptor(context.Background(), &refundv1.ListBanksRequest{}, &grpc.UnaryServerInfo{FullMethod: "/bridgepay.refund.v1.RefundGatewayService/ListBanks"}, func(ctx context.Context, _ any) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) < 4*time.Second {
			t.Fatalf("unexpected deadline: %v, present=%t", deadline, ok)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoggingInterceptorRedactsRequestBody(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	request := &refundv1.AcceptXenditCallbackRequest{
		Context:       &refundv1.RefundRequestContext{RequestId: "request-safe"},
		CallbackToken: "SECRET-CALLBACK-TOKEN", PayloadJson: []byte(`{"accountNumber":"SECRET-ACCOUNT"}`),
	}
	_, _ = loggingInterceptor(logger)(context.Background(), request, &grpc.UnaryServerInfo{FullMethod: "/bridgepay.refund.v1.RefundGatewayService/AcceptXenditCallback"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "callback authentication failed")
	})
	logged := output.String()
	if !strings.Contains(logged, "request-safe") || strings.Contains(logged, "SECRET-CALLBACK-TOKEN") || strings.Contains(logged, "SECRET-ACCOUNT") {
		t.Fatalf("unsafe access log: %s", logged)
	}
}
