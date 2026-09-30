//go:build integration

package rmqserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bridgepay-refund-go/internal/lifecycle"
)

func TestGatewayInteropServer(t *testing.T) {
	if os.Getenv("TEST_GATEWAY_INTEROP_SERVER") != "1" {
		t.Skip("interop server mode is not enabled")
	}
	rawURL := os.Getenv("TEST_RABBITMQ_URL")
	queue := os.Getenv("TEST_REFUND_QUEUE")
	if rawURL == "" || queue == "" {
		t.Fatal("TEST_RABBITMQ_URL and TEST_REFUND_QUEUE are required")
	}
	tracker := lifecycle.New()
	server, err := Dial(Config{URL: rawURL, Queue: queue, Prefetch: 4, RetryDelay: 100 * time.Millisecond, MaxRetries: 2}, slog.New(slog.NewTextHandler(io.Discard, nil)), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer deleteInteropQueues(rawURL, queue)
	stop := make(chan struct{}, 1)
	server.RegisterCommand("refund.bankList", func(context.Context, json.RawMessage) (any, error) {
		return map[string]any{"retCode": 0, "retData": []map[string]string{{"code": "BCA", "name": "Bank Central Asia"}}, "retMsg": "success"}, nil
	})
	server.RegisterCommand("refund.status", func(context.Context, json.RawMessage) (any, error) {
		return map[string]any{"retCode": 0, "retMsg": "success"}, nil
	})
	server.RegisterCommand("refund.create", func(context.Context, json.RawMessage) (any, error) {
		return nil, &ApplicationError{Payload: map[string]any{"retCode": -1, "retMsg": "Synthetic provider failure"}}
	})
	server.RegisterCommand("test.stop", func(context.Context, json.RawMessage) (any, error) {
		stop <- struct{}{}
		return map[string]bool{"stopping": true}, nil
	})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- server.Run(runCtx) }()
	fmt.Println("REFUND_GO_INTEROP_READY")
	select {
	case <-stop:
		time.Sleep(200 * time.Millisecond)
		_ = server.StopAccepting()
		tracker.BeginDrain()
		if err := tracker.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Gateway-Go did not finish the interoperability test")
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Refund-Go interoperability server did not drain")
	}
}

func deleteInteropQueues(rawURL, queue string) {
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		return
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	for _, name := range []string{queue, queue + ".retry", queue + ".dlq"} {
		_, _ = channel.QueueDelete(name, false, false, false)
	}
}
