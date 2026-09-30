//go:build integration

package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/refundbank"
)

type interopFixture struct{ called chan struct{} }

func (f *interopFixture) record() { f.called <- struct{}{} }
func (f *interopFixture) PublicList(context.Context) (refundbank.PublicListResponse, error) {
	f.record()
	return refundbank.PublicListResponse{RetCode: 0, RetMsg: "success", RetData: []refundbank.PublicBank{}}, nil
}
func (f *interopFixture) CheckAccount(context.Context, iluma.CheckAccountRequest) (iluma.CheckAccountResponse, error) {
	f.record()
	return iluma.CheckAccountResponse{RetCode: 0, Message: "Success", RetData: &iluma.CheckAccountData{Status: "success"}, SignMsg: "signed"}, nil
}
func (f *interopFixture) Callback(context.Context, json.RawMessage) (map[string]string, error) {
	f.record()
	return map[string]string{"message": "OK"}, nil
}
func (f *interopFixture) Create(context.Context, refund.CreateRequest) (refund.Response, error) {
	f.record()
	return refund.Response{RetCode: 0, RetMsg: "Success", RetData: refund.ResponseData{Invoice: refund.CreateInvoice{OrderID: "ORDER-1", Status: "PG process"}}, SignMsg: "signed"}, nil
}
func (f *interopFixture) Status(context.Context, refund.StatusRequest) (refund.Response, error) {
	f.record()
	return refund.Response{RetCode: 0, RetMsg: "success", RetData: refund.ResponseData{Invoice: refund.CreateInvoice{OrderID: "ORDER-1", Status: "success"}}, SignMsg: "signed"}, nil
}
func (f *interopFixture) Accept(context.Context, string, json.RawMessage) (map[string]string, error) {
	f.record()
	return map[string]string{"message": "OK"}, nil
}

func TestGatewayInteropServer(t *testing.T) {
	if os.Getenv("TEST_GATEWAY_REFUND_GRPC_INTEROP_SERVER") != "1" {
		t.Skip("interop server mode is not enabled")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &interopFixture{called: make(chan struct{}, 6)}
	server := New(nil, lifecycle.New(), slog.New(slog.NewTextHandler(io.Discard, nil)), Dependencies{Banks: fixture, Iluma: fixture, Refund: fixture, Webhook: fixture})
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	fmt.Printf("REFUND_GO_INTEROP_READY %s\n", listener.Addr())
	for range 6 {
		select {
		case <-fixture.called:
		case <-time.After(30 * time.Second):
			t.Fatal("Gateway-Go did not complete the interoperability calls")
		}
	}
	server.GracefulStop()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}
