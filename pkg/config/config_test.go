package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadBuildsEscapedDatabaseURLAndReadsSecretFiles(t *testing.T) {
	t.Setenv("NODE_ENV", "development")
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USERNAME", "refund user")
	t.Setenv("DB_PASSWORD", "p@ss/word")
	t.Setenv("DB_DATABASE", "refund")
	t.Setenv("DATABASE_URL", "")

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "private.pem")
	secretPath := filepath.Join(dir, "service-secret")
	if err := os.WriteFile(keyPath, []byte("synthetic key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secretPath, []byte("mounted-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KEY_FILE_PRIVATE", keyPath)
	t.Setenv("SERVICE_TO_SERVICE_SECRET_FILE", secretPath)
	t.Setenv("SERVICE_TO_SERVICE_SECRET", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceToServiceKey != "mounted-secret" {
		t.Fatalf("unexpected secret value %q", cfg.ServiceToServiceKey)
	}
	if !strings.Contains(cfg.DatabaseURL, "refund%20user:p%40ss%2Fword@") {
		t.Fatalf("database URL did not escape credentials: %s", cfg.DatabaseURL)
	}
}

func TestLoadRequiresSigningKeyInDevelopment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost/refund?sslmode=disable")
	t.Setenv("KEY_FILE_PRIVATE", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "KEY_FILE_PRIVATE") {
		t.Fatalf("expected signing-key error, got %v", err)
	}
}

func TestProductionRequiresOperationalSecrets(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "private.pem")
	if err := os.WriteFile(keyPath, []byte("synthetic key"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_ENV", "production")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost/refund?sslmode=require")
	t.Setenv("KEY_FILE_PRIVATE", keyPath)
	t.Setenv("SERVICE_TO_SERVICE_SECRET", "")
	t.Setenv("SERVICE_TO_SERVICE_SECRET_FILE", "")
	t.Setenv("ILUMA_TOKEN", "")
	t.Setenv("ILUMA_TOKEN_FILE", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "SERVICE_TO_SERVICE_SECRET") || !strings.Contains(err.Error(), "ILUMA_TOKEN") || !strings.Contains(err.Error(), "CORE_GRPC_TLS_CA_FILE") {
		t.Fatalf("expected production secret errors, got %v", err)
	}
}

func TestLoadReadsRabbitMQTransportControls(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "private.pem")
	if err := os.WriteFile(keyPath, []byte("synthetic key"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_ENV", "test")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost/refund?sslmode=disable")
	t.Setenv("KEY_FILE_PRIVATE", keyPath)
	t.Setenv("RABBITMQ_CONSUMER_ENABLED", "true")
	t.Setenv("RABBITMQ_PREFETCH", "7")
	t.Setenv("RABBITMQ_RETRY_DELAY", "125ms")
	t.Setenv("RABBITMQ_MAX_RETRIES", "2")
	t.Setenv("REPORT_SCHEDULER_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.RabbitMQConsumerEnabled || !cfg.ReportSchedulerEnabled || cfg.RabbitMQPrefetch != 7 || cfg.RabbitMQRetryDelay.String() != "125ms" || cfg.RabbitMQMaxRetries != 2 {
		t.Fatalf("transport controls = enabled:%t prefetch:%d delay:%s retries:%d", cfg.RabbitMQConsumerEnabled, cfg.RabbitMQPrefetch, cfg.RabbitMQRetryDelay, cfg.RabbitMQMaxRetries)
	}
}
