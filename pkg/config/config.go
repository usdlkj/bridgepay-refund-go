package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Environment       string
	HTTPAddress       string
	LogLevel          string
	ShutdownTimeout   time.Duration
	DependencyTimeout time.Duration

	DatabaseURL string
	RedisURL    string

	RabbitMQURL                    string
	RabbitMQRefundQueue            string
	RabbitMQEncryptorQueue         string
	RabbitMQConsumerEnabled        bool
	RabbitMQPrefetch               int
	RabbitMQRetryDelay             time.Duration
	RabbitMQMaxRetries             int
	ReportSchedulerEnabled         bool
	CoreGRPCAddress                string
	CoreGRPCTLSCAFile              string
	CoreGRPCTLSCertFile            string
	CoreGRPCTLSKeyFile             string
	CoreGRPCTLSServerName          string
	RefundGRPCListenAddress        string
	RefundGRPCTLSCertFile          string
	RefundGRPCTLSKeyFile           string
	RefundGRPCTLSClientCAFile      string
	RefundGRPCAllowedClientName    string
	RefundGRPCBackofficeClientName string
	BackofficeJWTSecret            string

	IlumaBaseURL         string
	XenditBaseURL        string
	ExternalHealthChecks bool
	IlumaHTTPTimeout     time.Duration
	IlumaCheckWait       time.Duration
	IlumaCheckMinSleep   time.Duration
	IlumaCheckMaxSleep   time.Duration
	BankAccountTTLDays   int
	DisbursementFeeFix   int64
	PPNValue             int64

	SigningPrivateKeyFile string
	SigningPrivateKeyPass string
	ServiceToServiceKey   string
	IlumaToken            string
}

func Load() (Config, error) {
	cfg := Config{
		Environment:                    envDefault("NODE_ENV", "development"),
		HTTPAddress:                    ":" + envDefault("PORT", "4000"),
		LogLevel:                       envDefault("LOG_LEVEL", "info"),
		RedisURL:                       envDefault("REDIS_URL", "redis://localhost:6379"),
		RabbitMQURL:                    envDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672"),
		RabbitMQRefundQueue:            envDefault("RABBITMQ_QUEUE", "bridgepay-refund"),
		RabbitMQEncryptorQueue:         envDefault("RABBITMQ_ENCRYPTOR_QUEUE", "bridgepay-encryptor"),
		CoreGRPCTLSCAFile:              strings.TrimSpace(os.Getenv("CORE_GRPC_TLS_CA_FILE")),
		CoreGRPCTLSCertFile:            strings.TrimSpace(os.Getenv("CORE_GRPC_TLS_CERT_FILE")),
		CoreGRPCTLSKeyFile:             strings.TrimSpace(os.Getenv("CORE_GRPC_TLS_KEY_FILE")),
		CoreGRPCTLSServerName:          strings.TrimSpace(os.Getenv("CORE_GRPC_TLS_SERVER_NAME")),
		CoreGRPCAddress:                envDefault("CORE_GRPC_ADDRESS", "localhost:50051"),
		RefundGRPCListenAddress:        envDefault("REFUND_GRPC_LISTEN_ADDRESS", ":50052"),
		RefundGRPCTLSCertFile:          strings.TrimSpace(os.Getenv("REFUND_GRPC_TLS_CERT_FILE")),
		RefundGRPCTLSKeyFile:           strings.TrimSpace(os.Getenv("REFUND_GRPC_TLS_KEY_FILE")),
		RefundGRPCTLSClientCAFile:      strings.TrimSpace(os.Getenv("REFUND_GRPC_TLS_CLIENT_CA_FILE")),
		RefundGRPCAllowedClientName:    strings.TrimSpace(os.Getenv("REFUND_GRPC_ALLOWED_CLIENT_NAME")),
		RefundGRPCBackofficeClientName: strings.TrimSpace(os.Getenv("REFUND_GRPC_BACKOFFICE_CLIENT_NAME")),
		IlumaBaseURL:                   envDefault("ILUMA_BASE_URL", "https://api.iluma.ai"),
		XenditBaseURL:                  envDefault("XENDIT_BASE_URL", "https://api.xendit.co"),
		SigningPrivateKeyFile:          strings.TrimSpace(os.Getenv("KEY_FILE_PRIVATE")),
		SigningPrivateKeyPass:          os.Getenv("KEY_FILE_PRIVATE_PASS"),
		ExternalHealthChecks:           true,
	}

	var err error
	cfg.ShutdownTimeout, err = positiveDuration("SHUTDOWN_TIMEOUT", "25s")
	if err != nil {
		return Config{}, err
	}
	cfg.DependencyTimeout, err = positiveDuration("DEPENDENCY_HEALTH_TIMEOUT", "2s")
	if err != nil {
		return Config{}, err
	}
	cfg.ExternalHealthChecks, err = boolEnv("EXTERNAL_HEALTH_CHECKS", true)
	if err != nil {
		return Config{}, err
	}
	cfg.RabbitMQConsumerEnabled, err = boolEnv("RABBITMQ_CONSUMER_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	cfg.RabbitMQPrefetch, err = positiveInt("RABBITMQ_PREFETCH", 16)
	if err != nil {
		return Config{}, err
	}
	cfg.RabbitMQRetryDelay, err = positiveDuration("RABBITMQ_RETRY_DELAY", "60s")
	if err != nil {
		return Config{}, err
	}
	cfg.RabbitMQMaxRetries, err = nonNegativeInt("RABBITMQ_MAX_RETRIES", 3)
	if err != nil {
		return Config{}, err
	}
	cfg.ReportSchedulerEnabled, err = boolEnv("REPORT_SCHEDULER_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	cfg.IlumaHTTPTimeout, err = positiveDuration("ILUMA_HTTP_TIMEOUT", "10s")
	if err != nil {
		return Config{}, err
	}
	cfg.IlumaCheckWait, err = positiveMilliseconds("CHECK_ACCOUNT_MAX_WAIT_MS", 7000)
	if err != nil {
		return Config{}, err
	}
	cfg.IlumaCheckMinSleep, err = positiveMilliseconds("CHECK_ACCOUNT_MIN_SLEEP_MS", 500)
	if err != nil {
		return Config{}, err
	}
	cfg.IlumaCheckMaxSleep, err = positiveMilliseconds("CHECK_ACCOUNT_MAX_SLEEP_MS", 2000)
	if err != nil {
		return Config{}, err
	}
	cfg.BankAccountTTLDays, err = positiveInt("BANK_ACCOUNT_CHECK_TTL_DAYS", 10)
	if err != nil {
		return Config{}, err
	}
	cfg.DisbursementFeeFix, err = nonNegativeInt64("DISBURSEMENT_FEE_FIX", 2100)
	if err != nil {
		return Config{}, err
	}
	cfg.PPNValue, err = nonNegativeInt64("PPN_VALUE", 11)
	if err != nil {
		return Config{}, err
	}

	cfg.DatabaseURL, err = databaseURL()
	if err != nil {
		return Config{}, err
	}
	cfg.ServiceToServiceKey, err = secret("SERVICE_TO_SERVICE_SECRET", "SERVICE_TO_SERVICE_SECRET_FILE")
	if err != nil {
		return Config{}, err
	}
	cfg.BackofficeJWTSecret, err = secret("BACKOFFICE_JWT_SECRET", "BACKOFFICE_JWT_SECRET_FILE")
	if err != nil {
		return Config{}, err
	}
	cfg.IlumaToken, err = secret("ILUMA_TOKEN", "ILUMA_TOKEN_FILE")
	if err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var problems []string
	if c.Environment != "development" && c.Environment != "test" && c.Environment != "staging" && c.Environment != "production" {
		problems = append(problems, "NODE_ENV must be development, test, staging, or production")
	}
	if _, port, err := net.SplitHostPort(c.HTTPAddress); err != nil || port == "" {
		problems = append(problems, "PORT must be a valid TCP port")
	}
	if _, port, err := net.SplitHostPort(c.RefundGRPCListenAddress); err != nil || port == "" {
		problems = append(problems, "REFUND_GRPC_LISTEN_ADDRESS must be a valid TCP address")
	}
	if c.ShutdownTimeout <= 0 {
		problems = append(problems, "SHUTDOWN_TIMEOUT must be positive")
	}
	if c.DependencyTimeout <= 0 {
		problems = append(problems, "DEPENDENCY_HEALTH_TIMEOUT must be positive")
	}
	if c.RabbitMQPrefetch <= 0 {
		problems = append(problems, "RABBITMQ_PREFETCH must be positive")
	}
	if c.RabbitMQRetryDelay <= 0 {
		problems = append(problems, "RABBITMQ_RETRY_DELAY must be positive")
	}
	if c.RabbitMQMaxRetries < 0 {
		problems = append(problems, "RABBITMQ_MAX_RETRIES must not be negative")
	}
	if c.IlumaHTTPTimeout <= 0 || c.IlumaCheckWait <= 0 || c.IlumaCheckMinSleep <= 0 || c.IlumaCheckMaxSleep <= 0 {
		problems = append(problems, "Iluma timeout and polling durations must be positive")
	}
	if c.IlumaCheckMinSleep > c.IlumaCheckMaxSleep {
		problems = append(problems, "CHECK_ACCOUNT_MIN_SLEEP_MS must not exceed CHECK_ACCOUNT_MAX_SLEEP_MS")
	}
	if c.BankAccountTTLDays <= 0 {
		problems = append(problems, "BANK_ACCOUNT_CHECK_TTL_DAYS must be positive")
	}
	if c.DisbursementFeeFix < 0 || c.PPNValue < 0 {
		problems = append(problems, "DISBURSEMENT_FEE_FIX and PPN_VALUE must not be negative")
	}
	for name, raw := range map[string]string{
		"DATABASE_URL": c.DatabaseURL,
		"REDIS_URL":    c.RedisURL,
		"RABBITMQ_URL": c.RabbitMQURL,
	} {
		if _, err := url.ParseRequestURI(raw); raw == "" || err != nil {
			problems = append(problems, name+" is invalid")
		}
	}
	for name, value := range map[string]string{
		"RABBITMQ_QUEUE":           c.RabbitMQRefundQueue,
		"RABBITMQ_ENCRYPTOR_QUEUE": c.RabbitMQEncryptorQueue,
		"CORE_GRPC_ADDRESS":        c.CoreGRPCAddress,
	} {
		if strings.TrimSpace(value) == "" {
			problems = append(problems, name+" is required")
		}
	}
	for name, raw := range map[string]string{"ILUMA_BASE_URL": c.IlumaBaseURL, "XENDIT_BASE_URL": c.XenditBaseURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
			problems = append(problems, name+" must be an absolute URL without embedded credentials")
		}
	}
	if c.SigningPrivateKeyFile == "" {
		problems = append(problems, "KEY_FILE_PRIVATE is required in every environment")
	} else if info, err := os.Stat(filepath.Clean(c.SigningPrivateKeyFile)); err != nil || !info.Mode().IsRegular() {
		problems = append(problems, "KEY_FILE_PRIVATE must reference a readable regular file")
	} else if file, err := os.Open(filepath.Clean(c.SigningPrivateKeyFile)); err != nil {
		problems = append(problems, "KEY_FILE_PRIVATE must reference a readable regular file")
	} else {
		_ = file.Close()
	}
	if c.Environment == "production" || c.Environment == "staging" {
		if !hasURLScheme(c.RedisURL, "rediss") {
			problems = append(problems, "REDIS_URL must use rediss://")
		}
		if !hasURLScheme(c.RabbitMQURL, "amqps") {
			problems = append(problems, "RABBITMQ_URL must use amqps://")
		}
		dbURL, err := url.Parse(c.DatabaseURL)
		if err != nil || dbURL.Query().Get("sslmode") != "verify-full" {
			problems = append(problems, "DATABASE_URL must use sslmode=verify-full")
		}
		if c.CoreGRPCTLSCAFile == "" || c.CoreGRPCTLSCertFile == "" || c.CoreGRPCTLSKeyFile == "" {
			problems = append(problems, "CORE_GRPC_TLS_CA_FILE, CORE_GRPC_TLS_CERT_FILE, and CORE_GRPC_TLS_KEY_FILE are required")
		}
		if c.ServiceToServiceKey == "" {
			problems = append(problems, "SERVICE_TO_SERVICE_SECRET or SERVICE_TO_SERVICE_SECRET_FILE is required")
		}
		if c.IlumaToken == "" {
			problems = append(problems, "ILUMA_TOKEN or ILUMA_TOKEN_FILE is required")
		}
		if c.RefundGRPCTLSCertFile == "" || c.RefundGRPCTLSKeyFile == "" || c.RefundGRPCTLSClientCAFile == "" || c.RefundGRPCAllowedClientName == "" {
			problems = append(problems, "REFUND_GRPC_TLS_CERT_FILE, REFUND_GRPC_TLS_KEY_FILE, REFUND_GRPC_TLS_CLIENT_CA_FILE, and REFUND_GRPC_ALLOWED_CLIENT_NAME are required")
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func hasURLScheme(raw, scheme string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == scheme && u.Host != ""
}

func databaseURL() (string, error) {
	if raw := strings.TrimSpace(os.Getenv("DATABASE_URL")); raw != "" {
		return raw, nil
	}
	required := []string{"DB_HOST", "DB_USERNAME", "DB_PASSWORD", "DB_DATABASE"}
	missing := make([]string, 0, len(required))
	for _, name := range required {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("missing required database settings: %s", strings.Join(missing, ", "))
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(os.Getenv("DB_USERNAME"), os.Getenv("DB_PASSWORD")),
		Host:   net.JoinHostPort(os.Getenv("DB_HOST"), envDefault("DB_PORT", "5432")),
		Path:   os.Getenv("DB_DATABASE"),
	}
	q := u.Query()
	q.Set("sslmode", envDefault("DB_SSL_MODE", "disable"))
	if ca := strings.TrimSpace(os.Getenv("DB_CA_PATH")); ca != "" {
		q.Set("sslrootcert", ca)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func secret(valueEnv, fileEnv string) (string, error) {
	value := os.Getenv(valueEnv)
	path := strings.TrimSpace(os.Getenv(fileEnv))
	if value != "" && path != "" {
		return "", fmt.Errorf("set only one of %s or %s", valueEnv, fileEnv)
	}
	if path == "" {
		return value, nil
	}
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", fileEnv, err)
	}
	return strings.TrimSpace(string(b)), nil
}

func positiveDuration(name, fallback string) (time.Duration, error) {
	d, err := time.ParseDuration(envDefault(name, fallback))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return d, nil
}

func boolEnv(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return v, nil
}

func positiveInt(name string, fallback int) (int, error) {
	value, err := integerEnv(name, fallback)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func positiveMilliseconds(name string, fallback int) (time.Duration, error) {
	value, err := positiveInt(name, fallback)
	if err != nil {
		return 0, err
	}
	return time.Duration(value) * time.Millisecond, nil
}

func nonNegativeInt(name string, fallback int) (int, error) {
	value, err := integerEnv(name, fallback)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func nonNegativeInt64(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func integerEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func envDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
