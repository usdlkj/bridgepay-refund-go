//go:build integration

package coreclient

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRefundClientTalksToCoreGoServer(t *testing.T) {
	coreRoot := os.Getenv("TEST_CORE_GO_ROOT")
	if coreRoot == "" {
		t.Skip("TEST_CORE_GO_ROOT is required")
	}
	serverCtx, cancelServer := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancelServer()
	command := exec.CommandContext(serverCtx, "go", "test", "-tags=integration", "./internal/grpcserver", "-run", "TestRefundInteropServer", "-count=1", "-v")
	command.Dir = filepath.Clean(coreRoot)
	command.Env = append(os.Environ(), "TEST_REFUND_CORE_GRPC_INTEROP_SERVER=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	address := ""
	for scanner.Scan() {
		line := scanner.Text()
		if marker := strings.Index(line, "CORE_REFUND_INTEROP_READY "); marker >= 0 {
			address = strings.TrimSpace(line[marker+len("CORE_REFUND_INTEROP_READY "):])
			break
		}
	}
	if address == "" {
		_ = command.Wait()
		t.Fatalf("Core-Go server did not become ready: %s", stderr.String())
	}

	client, err := Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	credential, err := client.PaymentGatewayCredential(context.Background(), "xendit")
	if err != nil || !strings.Contains(credential, "interop-secret") {
		t.Fatalf("credential=%q error=%v", credential, err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("Core-Go server failed: %v: %s", err, stderr.String())
	}
}
