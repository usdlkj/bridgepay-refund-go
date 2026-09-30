package grpcserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	refundv1 "bridgepay-refund-go/internal/coreclient/pb"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/refundbank"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type Dependencies struct {
	Banks interface {
		PublicList(context.Context) (refundbank.PublicListResponse, error)
	}
	Iluma interface {
		CheckAccount(context.Context, iluma.CheckAccountRequest) (iluma.CheckAccountResponse, error)
		Callback(context.Context, json.RawMessage) (map[string]string, error)
	}
	Refund interface {
		Create(context.Context, refund.CreateRequest) (refund.Response, error)
		Status(context.Context, refund.StatusRequest) (refund.Response, error)
	}
	Webhook interface {
		Accept(context.Context, string, json.RawMessage) (map[string]string, error)
	}
}

type service struct {
	refundv1.UnimplementedRefundGatewayServiceServer
	deps Dependencies
}

func New(tlsConfig *tls.Config, tracker *lifecycle.Tracker, logger *slog.Logger, deps Dependencies) *grpc.Server {
	if tracker == nil {
		tracker = lifecycle.New()
	}
	if logger == nil {
		logger = slog.Default()
	}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(1 << 20),
		grpc.MaxSendMsgSize(1 << 20),
		grpc.ChainUnaryInterceptor(admissionInterceptor(tracker), deadlineInterceptor(), loggingInterceptor(logger), recoveryInterceptor(logger)),
	}
	if tlsConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	server := grpc.NewServer(options...)
	refundv1.RegisterRefundGatewayServiceServer(server, &service{deps: deps})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(refundv1.RefundGatewayService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(server, healthServer)
	return server
}

func (s *service) ListBanks(ctx context.Context, request *refundv1.ListBanksRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil {
		return nil, status.Error(codes.InvalidArgument, "request context is required")
	}
	response, err := s.deps.Banks.PublicList(ctx)
	return encode(response, err)
}

func (s *service) CheckAccount(ctx context.Context, request *refundv1.CheckAccountRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil || request.GetAccount() == nil {
		return nil, status.Error(codes.InvalidArgument, "account request is required")
	}
	value := iluma.CheckAccountRequest{SignMsg: request.GetSignMsg()}
	value.ReqData.Account = iluma.Account{
		BankID: request.Account.GetBankId(), AccountNo: request.Account.GetAccountNo(), AccountType: request.Account.GetAccountType(),
		IDNo: request.Account.GetIdNo(), IDType: request.Account.GetIdType(), Name: request.Account.GetName(),
	}
	if problems := iluma.ValidateCheckAccount(value); len(problems) > 0 {
		return nil, status.Error(codes.InvalidArgument, strings.Join(problems, "; "))
	}
	response, err := s.deps.Iluma.CheckAccount(ctx, value)
	return encode(response, err)
}

func (s *service) AcceptIlumaCallback(ctx context.Context, request *refundv1.AcceptIlumaCallbackRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil || !json.Valid(request.GetPayloadJson()) {
		return nil, status.Error(codes.InvalidArgument, "valid callback JSON is required")
	}
	response, err := s.deps.Iluma.Callback(ctx, request.GetPayloadJson())
	return encode(response, err)
}

func (s *service) CreateRefund(ctx context.Context, request *refundv1.CreateRefundRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil || request.GetAccount() == nil || request.GetInvoice() == nil {
		return nil, status.Error(codes.InvalidArgument, "refund request is required")
	}
	value := refund.CreateRequest{SignMsg: request.GetSignMsg(), TicketCall: request.TicketCall}
	value.ReqData.Account = refund.Account{
		BankID: request.Account.GetBankId(), AccountNo: request.Account.GetAccountNo(), AccountType: request.Account.GetAccountType(),
		IDNo: request.Account.GetIdNo(), IDType: request.Account.GetIdType(), Name: request.Account.GetName(),
	}
	value.ReqData.Invoice = refund.Invoice{
		OrderID: request.Invoice.GetOrderId(), RefundAmount: request.Invoice.RefundAmount, Reason: request.Invoice.GetReason(),
		Passengers: request.Invoice.GetPassengers(), OriginalOrderNumber: request.Invoice.GetOriginalOrderNumber(),
		NotifyURL: request.Invoice.GetNotifyUrl(), TicketOffice: request.Invoice.GetTicketOffice(),
	}
	if problems := refund.ValidateCreate(value); len(problems) > 0 {
		return nil, status.Error(codes.InvalidArgument, strings.Join(problems, "; "))
	}
	response, err := s.deps.Refund.Create(ctx, value)
	return encode(response, err)
}

func (s *service) GetRefundStatus(ctx context.Context, request *refundv1.GetRefundStatusRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil {
		return nil, status.Error(codes.InvalidArgument, "status request is required")
	}
	value := refund.StatusRequest{SignMsg: request.GetSignMsg()}
	value.ReqData.Invoice.OrderID = request.GetOrderId()
	if problems := refund.ValidateStatus(value); len(problems) > 0 {
		return nil, status.Error(codes.InvalidArgument, strings.Join(problems, "; "))
	}
	response, err := s.deps.Refund.Status(ctx, value)
	return encode(response, err)
}

func (s *service) AcceptXenditCallback(ctx context.Context, request *refundv1.AcceptXenditCallbackRequest) (*refundv1.RefundHTTPResponse, error) {
	if request == nil || request.GetContext() == nil || !json.Valid(request.GetPayloadJson()) {
		return nil, status.Error(codes.InvalidArgument, "valid callback JSON is required")
	}
	response, err := s.deps.Webhook.Accept(ctx, request.GetCallbackToken(), request.GetPayloadJson())
	if err != nil {
		var public *refund.WebhookError
		if errors.As(err, &public) && public.HTTPStatus == 401 {
			return nil, status.Error(codes.Unauthenticated, "callback authentication failed")
		}
	}
	return encode(response, err)
}

func encode(value any, err error) (*refundv1.RefundHTTPResponse, error) {
	if err != nil {
		return nil, status.Error(codes.Internal, "refund request failed")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode refund response")
	}
	return &refundv1.RefundHTTPResponse{BodyJson: body}, nil
}

func admissionInterceptor(tracker *lifecycle.Tracker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method := info.FullMethod
		sideEffecting := !strings.HasSuffix(method, "/ListBanks") && !strings.HasSuffix(method, "/GetRefundStatus") && !strings.HasSuffix(method, "/Check")
		finish, accepted := tracker.TryBegin("grpc:"+method, sideEffecting)
		if !accepted {
			return nil, status.Error(codes.Unavailable, "refund service is draining")
		}
		defer finish()
		return handler(ctx, request)
	}
}

func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, request)
		logger.InfoContext(ctx, "gRPC request", "request_id", requestID(request), "method", info.FullMethod, "code", status.Code(err).String(), "latency_ms", time.Since(started).Milliseconds())
		return response, err
	}
}

func deadlineInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		budget := 10 * time.Second
		switch {
		case strings.HasSuffix(info.FullMethod, "/ListBanks"), strings.HasSuffix(info.FullMethod, "/GetRefundStatus"), strings.HasSuffix(info.FullMethod, "/Check"):
			budget = 5 * time.Second
		case strings.HasSuffix(info.FullMethod, "/CreateRefund"):
			budget = 30 * time.Second
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= budget {
			return handler(ctx, request)
		}
		callCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		return handler(callCtx, request)
	}
}

type requestContextCarrier interface {
	GetContext() *refundv1.RefundRequestContext
}

func requestID(request any) string {
	carrier, ok := request.(requestContextCarrier)
	if !ok || carrier.GetContext() == nil {
		return ""
	}
	return carrier.GetContext().GetRequestId()
}

func recoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(ctx, "gRPC panic", "method", info.FullMethod, "panic", recovered, "stack", string(debug.Stack()))
				response, err = nil, status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, request)
	}
}
