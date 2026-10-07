package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"bridgepay-refund-go/internal/backoffice"
	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/grpcserver"
	"bridgepay-refund-go/internal/health"
	"bridgepay-refund-go/internal/httpapi"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/lifecycle"
	"bridgepay-refund-go/internal/refund"
	"bridgepay-refund-go/internal/refundbank"
	refundreport "bridgepay-refund-go/internal/report"
	"bridgepay-refund-go/internal/rmqserver"
	"bridgepay-refund-go/internal/signing"
	"bridgepay-refund-go/internal/storage"
	"bridgepay-refund-go/pkg/config"
)

var version = "dev"

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration validation failed", "error", err)
		os.Exit(1)
	}
	logger := newLogger(cfg.LogLevel).With("service", "bridgepay-refund-go", "version", version)
	slog.SetDefault(logger)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database pool configuration failed", "error", "invalid database configuration")
		os.Exit(1)
	}
	defer pool.Close()

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("redis configuration failed", "error", "invalid redis URL")
		os.Exit(1)
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()

	tracker := lifecycle.New()
	coreTLS, err := coreclient.ClientTLSConfig(cfg.CoreGRPCTLSCAFile, cfg.CoreGRPCTLSCertFile, cfg.CoreGRPCTLSKeyFile, cfg.CoreGRPCTLSServerName)
	if err != nil {
		logger.Error("Core gRPC TLS configuration failed", "error", err)
		os.Exit(1)
	}
	coreClient, err := coreclient.Dial(cfg.CoreGRPCAddress, coreTLS)
	if err != nil {
		logger.Error("Core gRPC client configuration failed", "error", err)
		os.Exit(1)
	}
	defer coreClient.Close()
	store := storage.New(pool)
	signer, err := signing.Load(cfg.SigningPrivateKeyFile, cfg.SigningPrivateKeyPass)
	if err != nil {
		logger.Error("response signing initialization failed", "error", err)
		os.Exit(1)
	}
	encryptorClient, err := encryptor.Dial(cfg.RabbitMQURL, cfg.RabbitMQEncryptorQueue)
	if err != nil {
		logger.Error("encryptor RabbitMQ client startup failed", "error", err)
		os.Exit(1)
	}
	defer encryptorClient.Close()

	refundConsumer, err := rmqserver.Dial(rmqserver.Config{
		URL: cfg.RabbitMQURL, Queue: cfg.RabbitMQRefundQueue, Prefetch: cfg.RabbitMQPrefetch,
		RetryDelay: cfg.RabbitMQRetryDelay, MaxRetries: cfg.RabbitMQMaxRetries,
	}, logger, tracker)
	if err != nil {
		logger.Error("refund RabbitMQ transport startup failed", "error", err)
		os.Exit(1)
	}
	defer refundConsumer.Close()
	ilumaClient := iluma.NewClient(cfg.IlumaBaseURL, cfg.IlumaToken, cfg.IlumaHTTPTimeout)
	ilumaService := iluma.NewService(store, encryptorClient, signer, refundConsumer, ilumaClient, iluma.Config{
		BaseURL: cfg.IlumaBaseURL, TTLDays: cfg.BankAccountTTLDays, MaxWait: cfg.IlumaCheckWait,
		MinSleep: cfg.IlumaCheckMinSleep, MaxSleep: cfg.IlumaCheckMaxSleep,
	}, logger)
	bankService := refundbank.New(store, coreClient, refundbank.NewXenditClient(cfg.XenditBaseURL), ilumaClient, cfg.Environment, logger)
	refundService := refund.New(store, coreClient, encryptorClient, signer, refund.NewXenditClient(cfg.XenditBaseURL), refund.Config{
		Environment: cfg.Environment, FeeFix: cfg.DisbursementFeeFix, PPNValue: cfg.PPNValue,
	}, logger)
	webhookService := refund.NewWebhookService(store, coreClient, refund.NewXenditClient(cfg.XenditBaseURL),
		refund.NewTicketingClient(), signer, refundConsumer, cfg.Environment, logger)
	backofficeService := backoffice.New(store, coreClient, encryptorClient, refund.NewXenditClient(cfg.XenditBaseURL), cfg.Environment)
	reportService := refundreport.New(store, func(kind string, job func(context.Context) error) bool {
		done, ok := tracker.TryBegin(kind, true)
		if !ok {
			return false
		}
		go func() {
			defer done()
			if err := job(context.Background()); err != nil {
				logger.Error("background job failed", "kind", kind, "error", err)
			}
		}()
		return true
	})
	registerBackgroundHandlers(refundConsumer, ilumaService, webhookService, cfg.RabbitMQMaxRetries)
	grpcTLS, err := grpcserver.ServerTLSConfig(cfg.RefundGRPCTLSCertFile, cfg.RefundGRPCTLSKeyFile, cfg.RefundGRPCTLSClientCAFile, cfg.RefundGRPCAllowedClientName, cfg.RefundGRPCBackofficeClientName)
	if err != nil {
		logger.Error("Refund gRPC TLS configuration failed", "error", err)
		os.Exit(1)
	}
	grpcListener, err := net.Listen("tcp", cfg.RefundGRPCListenAddress)
	if err != nil {
		logger.Error("Refund gRPC listener failed", "error", err)
		os.Exit(1)
	}
	grpcService := grpcserver.New(grpcTLS, tracker, logger, grpcserver.Dependencies{
		BackofficeJWTSecret:  cfg.BackofficeJWTSecret,
		GatewayClientName:    cfg.RefundGRPCAllowedClientName,
		BackofficeClientName: cfg.RefundGRPCBackofficeClientName,
		Banks:                bankService, Backoffice: backofficeService, Iluma: ilumaService, Refund: refundService, Webhook: webhookService,
	})
	grpcErrors := make(chan error, 1)
	go func() {
		logger.Info("gRPC server started", "address", cfg.RefundGRPCListenAddress)
		grpcErrors <- grpcService.Serve(grpcListener)
	}()
	schedulerCtx, stopScheduler := context.WithCancel(context.Background())
	defer stopScheduler()
	if cfg.ReportSchedulerEnabled {
		if err := reportService.Resume(ctx); err != nil {
			logger.Error("pending report recovery failed", "error", err)
			os.Exit(1)
		}
		refundreport.NewScheduler(reportService, logger).Run(schedulerCtx)
		logger.Info("report scheduler started", "timezone", "Asia/Jakarta")
	}

	transportErrors := make(chan error, 1)
	if cfg.RabbitMQConsumerEnabled {
		go func() { transportErrors <- refundConsumer.Run(context.Background()) }()
		logger.Info("refund RabbitMQ consumer started", "queue", cfg.RabbitMQRefundQueue, "prefetch", cfg.RabbitMQPrefetch)
	}
	healthService := health.New(cfg.DependencyTimeout,
		health.PostgreSQL(pool),
		health.Check{Name: "refund_schema", Run: func(ctx context.Context) error { return storage.VerifySchema(ctx, pool) }},
		health.Redis(redisClient),
		health.RabbitMQ(cfg.RabbitMQURL),
		health.RabbitMQQueue(cfg.RabbitMQURL, "encryptor", cfg.RabbitMQEncryptorQueue),
		health.RabbitMQQueue(cfg.RabbitMQURL, "refund_queue", cfg.RabbitMQRefundQueue),
		health.TCP("core", cfg.CoreGRPCAddress),
		health.HTTPReachability("iluma", cfg.IlumaBaseURL, cfg.ExternalHealthChecks),
		health.HTTPReachability("xendit", cfg.XenditBaseURL, cfg.ExternalHealthChecks),
	)

	server := &http.Server{
		Addr: cfg.HTTPAddress,
		Handler: httpapi.New(logger, healthService, tracker, httpapi.Dependencies{
			Banks: bankService, Iluma: ilumaService, Refund: refundService, Webhook: webhookService,
			Backoffice: backofficeService, Reports: reportService, ServiceKey: cfg.ServiceToServiceKey,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server started", "address", cfg.HTTPAddress, "environment", cfg.Environment)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
			return
		}
		serverErrors <- nil
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	exitCode := 0
	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if err != nil {
			logger.Error("http server failed", "error", err)
			exitCode = 1
		}
	case err := <-transportErrors:
		if err != nil {
			logger.Error("refund RabbitMQ consumer failed", "error", err)
			exitCode = 1
		}
	case err := <-grpcErrors:
		if err != nil {
			logger.Error("Refund gRPC server failed", "error", err)
			exitCode = 1
		}
	}

	started := time.Now()
	stopScheduler()
	if cfg.RabbitMQConsumerEnabled {
		if err := refundConsumer.StopAccepting(); err != nil {
			logger.Warn("refund RabbitMQ admission stop failed", "error", err)
		}
	}
	snapshot := tracker.BeginDrain()
	logger.Info("drain started",
		"in_flight", snapshot.InFlight,
		"side_effecting", snapshot.SideEffecting,
		"by_kind", snapshot.ByKind,
	)
	grpcStopped := make(chan struct{})
	go func() {
		grpcService.GracefulStop()
		close(grpcStopped)
	}()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http drain incomplete", "error", err)
	}
	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		grpcService.Stop()
	}
	if err := tracker.Wait(shutdownCtx); err != nil {
		unfinished := tracker.Snapshot()
		logger.Error("shutdown deadline reached with unfinished work",
			"in_flight", unfinished.InFlight,
			"side_effecting", unfinished.SideEffecting,
			"by_kind", unfinished.ByKind,
		)
		os.Exit(1)
	}
	logger.Info("shutdown complete", "duration_ms", time.Since(started).Milliseconds())
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func registerBackgroundHandlers(server *rmqserver.Server, validation *iluma.Service, webhooks *refund.WebhookService, maxRetries int) {
	server.RegisterEvent("refund.iluma.poll", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var request iluma.PollRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		return nil, validation.Poll(ctx, request)
	})
	server.RegisterEvent("refund.xendit.callback.process", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var request struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &request); err != nil || request.ID == "" {
			return nil, errors.New("refund callback ID is required")
		}
		metadata, _ := rmqserver.MetadataFromContext(ctx)
		return nil, webhooks.Process(ctx, request.ID, metadata.RetryCount, maxRetries)
	})
}

func newLogger(rawLevel string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(rawLevel)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
