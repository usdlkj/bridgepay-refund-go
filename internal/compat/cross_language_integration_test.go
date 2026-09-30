//go:build integration

package compat

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"bridgepay-refund-go/internal/encryptor"
	"bridgepay-refund-go/internal/storage"
)

func TestNodeAndGoEncryptedRowsAreMutuallyReadable(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" && os.Getenv("TEST_DB_USERNAME") != "" {
		u := &url.URL{Scheme: "postgres", User: url.UserPassword(os.Getenv("TEST_DB_USERNAME"), os.Getenv("TEST_DB_PASSWORD")), Host: net.JoinHostPort(envDefault("TEST_DB_HOST", "127.0.0.1"), envDefault("TEST_DB_PORT", "5432")), Path: envDefault("TEST_DB_DATABASE", "kcic_refund")}
		query := u.Query()
		query.Set("sslmode", "disable")
		u.RawQuery = query.Encode()
		databaseURL = u.String()
	}
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	nodeRoot := os.Getenv("TEST_NODE_REFUND_ROOT")
	encryptorRoot := os.Getenv("TEST_NODE_ENCRYPTOR_ROOT")
	if databaseURL == "" || rabbitURL == "" || nodeRoot == "" || encryptorRoot == "" {
		t.Skip("database, RabbitMQ, Node refund, and Node encryptor test settings are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := storage.New(pool)
	client, err := encryptor.Dial(rabbitURL, "bridgepay-encryptor")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve compatibility script path")
	}
	scriptRoot := filepath.Join(filepath.Dir(currentFile), "..", "..", "scripts")
	nodeID := storage.NewID()
	nodeSynthetic := "SYNTHETIC-NODE-ACCOUNT-" + storage.NewID()
	createCommand := exec.CommandContext(ctx, "node", filepath.Join(scriptRoot, "node-create-compat.cjs"))
	createCommand.Env = append(os.Environ(),
		"COMPAT_BANK_DATA_ID="+nodeID,
		"COMPAT_EXPECTED_VALUE="+nodeSynthetic,
		"NODE_REFUND_ROOT="+nodeRoot,
		"NODE_ENCRYPTOR_ROOT="+encryptorRoot,
		"DATABASE_URL="+databaseURL,
		"RABBITMQ_URL="+rabbitURL,
		"RABBITMQ_ENCRYPTOR_QUEUE=bridgepay-encryptor",
	)
	if output, err := createCommand.CombinedOutput(); err != nil || !strings.HasSuffix(string(output), "node-create-compatible\n") {
		t.Fatalf("Node could not create compatibility row: %v: %s", err, output)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM bank_datas WHERE id = $1 AND bank_code = 'SYNTHETIC_NODE'`, nodeID)
	}()
	nodeRecord, err := store.GetBankDataByID(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	var nodeCiphertext encryptor.Ciphertext
	if err := json.Unmarshal(nodeRecord.AccountNumberEnc, &nodeCiphertext); err != nil {
		t.Fatalf("decode Node ciphertext: %v", err)
	}
	plaintext, err := client.Decrypt(ctx, nodeCiphertext)
	if err != nil {
		t.Fatalf("Go could not decrypt Node-created row: %v", err)
	}
	if plaintext == "" {
		t.Fatal("Node-created row decrypted to empty data")
	}
	if plaintext != nodeSynthetic {
		t.Fatal("Node-created row decrypted to the wrong synthetic value")
	}
	blindIndex, err := client.BlindIndex(ctx, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if blindIndex != nodeRecord.AccountNumberHash {
		t.Fatal("Node-created row blind index differs from Go result")
	}
	plaintext = "" // do not retain or print sensitive data

	synthetic := "SYNTHETIC-ACCOUNT-" + storage.NewID()
	ciphertext, err := client.Encrypt(ctx, synthetic)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := client.BlindIndex(ctx, synthetic)
	if err != nil {
		t.Fatal(err)
	}
	ciphertextJSON, err := json.Marshal(ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	goRecord, created, err := store.GetOrCreateBankData(ctx, storage.BankDataInsert{
		BankCode: "SYNTHETIC_COMPAT", AccountNumberEnc: ciphertextJSON, AccountNumberHash: hash,
	})
	if err != nil || !created {
		t.Fatalf("create Go compatibility row: created=%v err=%v", created, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM bank_datas WHERE id = $1 AND bank_code = 'SYNTHETIC_COMPAT'`, goRecord.ID)
	}()

	script := filepath.Join(scriptRoot, "node-read-compat.cjs")
	command := exec.CommandContext(ctx, "node", script)
	command.Env = append(os.Environ(),
		"COMPAT_BANK_DATA_ID="+goRecord.ID,
		"COMPAT_EXPECTED_VALUE="+synthetic,
		"NODE_REFUND_ROOT="+nodeRoot,
		"NODE_ENCRYPTOR_ROOT="+encryptorRoot,
		"DATABASE_URL="+databaseURL,
		"RABBITMQ_URL="+rabbitURL,
		"RABBITMQ_ENCRYPTOR_QUEUE=bridgepay-encryptor",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Node could not read/decrypt Go-created row: %v: %s", err, output)
	}
	if !strings.HasSuffix(string(output), "node-read-compatible\n") {
		t.Fatalf("unexpected Node compatibility output: %q", output)
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
