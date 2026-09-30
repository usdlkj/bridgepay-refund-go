package rmqserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bridgepay-refund-go/internal/lifecycle"
)

func TestNestRPCNoReplayAndBackgroundRetry(t *testing.T) {
	rawURL := os.Getenv("TEST_RABBITMQ_URL")
	if rawURL == "" {
		t.Skip("TEST_RABBITMQ_URL not set")
	}
	queue := fmt.Sprintf("bridgepay-refund-go-test-%d", time.Now().UnixNano())
	tracker := lifecycle.New()
	server, err := Dial(Config{URL: rawURL, Queue: queue, Prefetch: 4, RetryDelay: 100 * time.Millisecond, MaxRetries: 2}, slog.New(slog.NewTextHandler(io.Discard, nil)), tracker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		admin, dialErr := amqp.Dial(rawURL)
		if dialErr != nil {
			return
		}
		defer admin.Close()
		adminChannel, channelErr := admin.Channel()
		if channelErr != nil {
			return
		}
		defer adminChannel.Close()
		for _, name := range []string{queue, queue + ".retry", queue + ".dlq"} {
			_, _ = adminChannel.QueueDelete(name, false, false, false)
		}
	})
	var createCalls atomic.Int32
	metadataSeen := make(chan Metadata, 1)
	server.RegisterCommand("refund.bankList", func(ctx context.Context, payload json.RawMessage) (any, error) {
		metadata, _ := MetadataFromContext(ctx)
		metadataSeen <- metadata
		return map[string]any{"data": []string{"BCA"}}, nil
	})
	server.RegisterCommand("refund.create", func(_ context.Context, payload json.RawMessage) (any, error) {
		createCalls.Add(1)
		return nil, &ApplicationError{Payload: map[string]any{"statusCode": 503, "message": "provider uncertain"}}
	})
	var pollCalls atomic.Int32
	pollDone := make(chan struct{}, 1)
	server.RegisterEvent("refund.iluma.poll", func(_ context.Context, _ json.RawMessage) (any, error) {
		if pollCalls.Add(1) < 3 {
			return nil, errors.New("temporary poll failure")
		}
		pollDone <- struct{}{}
		return nil, nil
	})
	var exhaustedCalls atomic.Int32
	server.RegisterEvent("refund.iluma.poll.exhausted", func(_ context.Context, _ json.RawMessage) (any, error) {
		exhaustedCalls.Add(1)
		return nil, errors.New("persistent poll failure")
	})
	publishedEvent := make(chan Metadata, 1)
	server.RegisterEvent("test.publish", func(ctx context.Context, _ json.RawMessage) (any, error) {
		metadata, _ := MetadataFromContext(ctx)
		publishedEvent <- metadata
		return nil, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- server.Run(ctx) }()
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := server.PublishEvent(publishCtx, "test.publish", map[string]string{"value": "ok"}, "step95:stable"); err != nil {
		publishCancel()
		t.Fatal(err)
	}
	publishCancel()
	select {
	case metadata := <-publishedEvent:
		if metadata.Pattern != "test.publish" || metadata.MessageID != "step95:stable" || metadata.IdempotencyKey != "step95:stable" {
			t.Fatalf("published metadata = %#v", metadata)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("published event was not consumed")
	}

	clientConn, err := amqp.Dial(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	clientChannel, err := clientConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	replies, err := clientChannel.Consume("amq.rabbitmq.reply-to", "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	call := func(command, id string) replyEnvelope {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"pattern": map[string]string{"cmd": command}, "data": map[string]string{"orderId": "ORDER-1"}, "id": id})
		if err := clientChannel.PublishWithContext(context.Background(), "", queue, true, false, amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: id,
			ReplyTo: "amq.rabbitmq.reply-to", MessageId: command + ":stable", Headers: amqp.Table{"x-idempotency-key": command + ":stable"}, Body: body,
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case delivery := <-replies:
			if delivery.CorrelationId != id {
				t.Fatalf("correlation id = %q, want %q", delivery.CorrelationId, id)
			}
			var response replyEnvelope
			if err := json.Unmarshal(delivery.Body, &response); err != nil {
				t.Fatal(err)
			}
			return response
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for direct reply")
			return replyEnvelope{}
		}
	}

	success := call("refund.bankList", "rpc-success")
	if !success.IsDisposed || success.Response == nil || success.Err != nil {
		t.Fatalf("success response = %#v", success)
	}
	metadata := <-metadataSeen
	if metadata.Pattern != "refund.bankList" || metadata.RequestID != "rpc-success" || metadata.CorrelationID != "rpc-success" || metadata.IdempotencyKey != "refund.bankList:stable" {
		t.Fatalf("handler metadata = %#v", metadata)
	}
	failure := call("refund.create", "rpc-side-effect")
	failureObject, ok := failure.Err.(map[string]any)
	if !failure.IsDisposed || !ok || failureObject["message"] != "provider uncertain" {
		t.Fatalf("failure response = %#v", failure)
	}
	time.Sleep(350 * time.Millisecond)
	if createCalls.Load() != 1 {
		t.Fatalf("refund.create automatically replayed: calls=%d", createCalls.Load())
	}

	eventBody, _ := json.Marshal(map[string]any{"pattern": "refund.iluma.poll", "data": map[string]string{"requestId": "REQUEST-1"}})
	if err := clientChannel.PublishWithContext(context.Background(), "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: "poll:stable", Body: eventBody,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pollDone:
	case <-time.After(4 * time.Second):
		t.Fatalf("background retry did not finish; calls=%d", pollCalls.Load())
	}
	if pollCalls.Load() != 3 {
		t.Fatalf("poll calls=%d, want 3", pollCalls.Load())
	}
	deadline := time.Now().Add(time.Second)
	stats := server.Stats()
	for stats.Completed < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		stats = server.Stats()
	}
	if stats.Retried != 2 || stats.RepliesFailed != 0 || stats.Completed < 2 {
		t.Fatalf("stats=%+v", stats)
	}

	exhaustedBody, _ := json.Marshal(map[string]any{"pattern": "refund.iluma.poll.exhausted", "data": map[string]string{"requestId": "REQUEST-DLQ"}})
	if err := clientChannel.PublishWithContext(context.Background(), "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: "poll:exhausted:stable", Body: exhaustedBody,
	}); err != nil {
		t.Fatal(err)
	}
	var deadLetter amqp.Delivery
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		message, ok, getErr := clientChannel.Get(queue+".dlq", true)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if ok {
			deadLetter = message
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(deadLetter.Body) == 0 {
		t.Fatalf("retry-exhausted event was not dead-lettered; calls=%d", exhaustedCalls.Load())
	}
	if exhaustedCalls.Load() != 3 || deadLetter.MessageId != "poll:exhausted:stable" || retryCount(deadLetter.Headers) != 2 || deadLetter.Headers["x-dead-letter-reason"] != "retry-exhausted" {
		t.Fatalf("dead letter calls=%d id=%q headers=%#v", exhaustedCalls.Load(), deadLetter.MessageId, deadLetter.Headers)
	}

	if err := server.StopAccepting(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop")
	}
	tracker.BeginDrain()
	if err := tracker.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = clientChannel.Close()
	_ = clientConn.Close()
}
