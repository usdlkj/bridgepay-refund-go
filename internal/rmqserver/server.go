// Package rmqserver implements Refund-Go's durable background-event retry
// path. Generic request/reply decoding remains only to drain migration-era
// messages; Gateway-Go no longer registers or uses synchronous commands.
package rmqserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bridgepay-refund-go/internal/lifecycle"
)

type Handler func(context.Context, json.RawMessage) (any, error)

type Metadata struct {
	Pattern        string
	RequestID      string
	CorrelationID  string
	MessageID      string
	IdempotencyKey string
	RetryCount     int
}

type metadataContextKey struct{}

func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	metadata, ok := ctx.Value(metadataContextKey{}).(Metadata)
	return metadata, ok
}

type Config struct {
	URL         string
	Queue       string
	Prefetch    int
	RetryDelay  time.Duration
	MaxRetries  int
	ConsumerTag string
}

type Stats struct {
	Received      uint64
	Completed     uint64
	Failed        uint64
	Retried       uint64
	DeadLettered  uint64
	RepliesFailed uint64
}

// ApplicationError allows business handlers to preserve a Node-compatible
// error object instead of reducing every error to a string.
type ApplicationError struct {
	Payload any
}

func (e *ApplicationError) Error() string {
	if message, ok := e.Payload.(string); ok {
		return message
	}
	return "refund request failed"
}

type envelope struct {
	Pattern json.RawMessage `json:"pattern"`
	Data    json.RawMessage `json:"data"`
	ID      string          `json:"id"`
}

type replyEnvelope struct {
	Response   any  `json:"response,omitempty"`
	Err        any  `json:"err,omitempty"`
	IsDisposed bool `json:"isDisposed"`
}

type Server struct {
	cfg     Config
	logger  *slog.Logger
	tracker *lifecycle.Tracker
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns <-chan amqp.Return

	mu          sync.RWMutex
	commands    map[string]Handler
	events      map[string]Handler
	publishMu   sync.Mutex
	stopOnce    sync.Once
	closeOnce   sync.Once
	consumerTag string

	received      atomic.Uint64
	completed     atomic.Uint64
	failed        atomic.Uint64
	retried       atomic.Uint64
	deadLettered  atomic.Uint64
	repliesFailed atomic.Uint64
}

func Dial(cfg Config, logger *slog.Logger, tracker *lifecycle.Tracker) (*Server, error) {
	if strings.TrimSpace(cfg.Queue) == "" {
		return nil, errors.New("refund queue is required")
	}
	if cfg.Prefetch <= 0 {
		return nil, errors.New("RabbitMQ prefetch must be positive")
	}
	if cfg.RetryDelay <= 0 {
		return nil, errors.New("RabbitMQ retry delay must be positive")
	}
	if cfg.MaxRetries < 0 {
		return nil, errors.New("RabbitMQ max retries must not be negative")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if tracker == nil {
		tracker = lifecycle.New()
	}
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connect refund RabbitMQ consumer: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open refund RabbitMQ channel: %w", err)
	}
	fail := func(err error) (*Server, error) {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	// The Node service already declares this queue without arguments. Keep that
	// declaration byte-for-byte compatible; retry topology is separate.
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("declare refund queue: %w", err))
	}
	retryQueue := cfg.Queue + ".retry"
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
		"x-message-ttl":             cfg.RetryDelay.Milliseconds(),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": cfg.Queue,
	}); err != nil {
		return fail(fmt.Errorf("declare refund retry queue: %w", err))
	}
	if _, err := ch.QueueDeclare(cfg.Queue+".dlq", true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("declare refund dead-letter queue: %w", err))
	}
	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fail(fmt.Errorf("configure refund prefetch: %w", err))
	}
	if err := ch.Confirm(false); err != nil {
		return fail(fmt.Errorf("enable refund publisher confirms: %w", err))
	}
	tag := cfg.ConsumerTag
	if tag == "" {
		tag = "bridgepay-refund-go"
	}
	s := &Server{
		cfg: cfg, logger: logger, tracker: tracker, conn: conn, ch: ch,
		returns:  ch.NotifyReturn(make(chan amqp.Return, 1)),
		commands: make(map[string]Handler), events: make(map[string]Handler), consumerTag: tag,
	}
	return s, nil
}

func (s *Server) RegisterCommand(pattern string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands[pattern] = handler
}

func (s *Server) RegisterEvent(pattern string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[pattern] = handler
}

// PublishEvent emits the same envelope produced by NestJS ClientProxy.emit.
// Publisher confirms make the hand-off durable before the caller continues.
func (s *Server) PublishEvent(ctx context.Context, pattern string, data any, messageID string) error {
	if s == nil || s.ch == nil {
		return errors.New("refund RabbitMQ publisher is unavailable")
	}
	if strings.TrimSpace(pattern) == "" {
		return errors.New("event pattern is required")
	}
	if strings.TrimSpace(messageID) == "" {
		return errors.New("event message ID is required")
	}
	body, err := json.Marshal(struct {
		Pattern string `json:"pattern"`
		Data    any    `json:"data"`
	}{Pattern: pattern, Data: data})
	if err != nil {
		return fmt.Errorf("encode RabbitMQ event: %w", err)
	}
	return s.publishConfirmed(ctx, s.cfg.Queue, true, amqp.Publishing{
		Headers:      amqp.Table{"x-idempotency-key": messageID},
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
}

func (s *Server) Run(ctx context.Context) error {
	deliveries, err := s.ch.Consume(s.cfg.Queue, s.consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume refund queue: %w", err)
	}
	closed := s.conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case brokerErr := <-closed:
			if brokerErr == nil || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("refund RabbitMQ connection closed: %w", brokerErr)
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			s.received.Add(1)
			go s.handle(ctx, delivery)
		}
	}
}

func (s *Server) handle(parent context.Context, delivery amqp.Delivery) {
	var packet envelope
	if err := json.Unmarshal(delivery.Body, &packet); err != nil {
		s.failed.Add(1)
		s.deadLetter(parent, delivery, "invalid-envelope")
		return
	}
	pattern, err := patternName(packet.Pattern)
	if err != nil {
		s.failed.Add(1)
		if delivery.ReplyTo != "" {
			s.finishRPC(parent, delivery, nil, err)
		} else {
			s.deadLetter(parent, delivery, "invalid-pattern")
		}
		return
	}
	isRPC := delivery.ReplyTo != ""
	handler := s.handler(pattern, isRPC)
	if handler == nil {
		err := fmt.Errorf("no handler registered for %s", pattern)
		s.failed.Add(1)
		if isRPC {
			s.finishRPC(parent, delivery, nil, err)
		} else {
			s.deadLetter(parent, delivery, "handler-not-found")
		}
		return
	}
	finish, accepted := s.tracker.TryBegin("rabbitmq:"+pattern, isSideEffecting(pattern))
	if !accepted {
		_ = delivery.Nack(false, true)
		return
	}
	defer finish()
	messageID := stableMessageID(delivery)
	metadata := Metadata{
		Pattern: pattern, RequestID: packet.ID, CorrelationID: delivery.CorrelationId,
		MessageID: messageID, IdempotencyKey: messageID, RetryCount: retryCount(delivery.Headers),
	}
	handlerContext := context.WithValue(parent, metadataContextKey{}, metadata)
	result, callErr := handler(handlerContext, packet.Data)
	if callErr != nil {
		s.failed.Add(1)
		if isRPC {
			// RPCs are never broker-replayed after handler execution. This is
			// mandatory for refund.create, which may already have reached Xendit.
			s.finishRPC(parent, delivery, nil, callErr)
			return
		}
		s.retryOrDeadLetter(parent, delivery, callErr)
		return
	}
	if isRPC {
		s.finishRPC(parent, delivery, result, nil)
	} else if err := delivery.Ack(false); err != nil {
		s.logger.Error("RabbitMQ event acknowledgement failed", "pattern", pattern, "error", err)
	}
	s.completed.Add(1)
}

func (s *Server) finishRPC(ctx context.Context, delivery amqp.Delivery, response any, callErr error) {
	packet := replyEnvelope{Response: response, IsDisposed: true}
	if callErr != nil {
		packet.Response = nil
		packet.Err = errorPayload(callErr)
	}
	body, err := json.Marshal(packet)
	if err == nil {
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = s.publishConfirmed(publishCtx, delivery.ReplyTo, false, amqp.Publishing{
			ContentType: "application/json", CorrelationId: delivery.CorrelationId, Body: body,
		})
		cancel()
	}
	if err != nil {
		s.repliesFailed.Add(1)
		s.logger.Error("RabbitMQ RPC reply failed; request will not be replayed", "correlation_id", delivery.CorrelationId, "error", err)
	}
	// Even if the direct reply is lost, replaying a possibly side-effecting RPC
	// is less safe than letting Gateway time out and reconcile by persisted ID.
	if ackErr := delivery.Ack(false); ackErr != nil {
		s.logger.Error("RabbitMQ RPC acknowledgement failed", "correlation_id", delivery.CorrelationId, "error", ackErr)
	}
}

func (s *Server) retryOrDeadLetter(ctx context.Context, delivery amqp.Delivery, _ error) {
	retries := retryCount(delivery.Headers)
	if retries >= s.cfg.MaxRetries {
		s.deadLetter(ctx, delivery, "retry-exhausted")
		return
	}
	headers := cloneHeaders(delivery.Headers)
	headers["x-retry-count"] = int64(retries + 1)
	headers["x-original-message-id"] = stableMessageID(delivery)
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.publishConfirmed(publishCtx, s.cfg.Queue+".retry", true, republish(delivery, headers))
	cancel()
	if err != nil {
		s.logger.Error("RabbitMQ background retry publish failed", "message_id", stableMessageID(delivery), "error", err)
		_ = delivery.Nack(false, true)
		return
	}
	s.retried.Add(1)
	_ = delivery.Ack(false)
}

func (s *Server) deadLetter(ctx context.Context, delivery amqp.Delivery, reason string) {
	headers := cloneHeaders(delivery.Headers)
	headers["x-dead-letter-reason"] = reason
	headers["x-original-message-id"] = stableMessageID(delivery)
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.publishConfirmed(publishCtx, s.cfg.Queue+".dlq", true, republish(delivery, headers))
	cancel()
	if err != nil {
		s.logger.Error("RabbitMQ dead-letter publish failed", "message_id", stableMessageID(delivery), "error", err)
		_ = delivery.Nack(false, true)
		return
	}
	s.deadLettered.Add(1)
	_ = delivery.Ack(false)
}

func (s *Server) publishConfirmed(ctx context.Context, routingKey string, mandatory bool, publishing amqp.Publishing) error {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	for {
		select {
		case <-s.returns:
		default:
			goto drained
		}
	}
drained:
	confirmation, err := s.ch.PublishWithDeferredConfirmWithContext(ctx, "", routingKey, mandatory, false, publishing)
	if err != nil {
		return err
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	select {
	case returned := <-s.returns:
		return fmt.Errorf("message was unroutable: %s", returned.ReplyText)
	default:
	}
	if !acknowledged {
		return errors.New("RabbitMQ negatively acknowledged publish")
	}
	return nil
}

func (s *Server) handler(pattern string, rpc bool) Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rpc {
		return s.commands[pattern]
	}
	return s.events[pattern]
}

func patternName(raw json.RawMessage) (string, error) {
	var value string
	if json.Unmarshal(raw, &value) == nil && value != "" {
		return value, nil
	}
	var object struct {
		Cmd string `json:"cmd"`
	}
	if json.Unmarshal(raw, &object) == nil && object.Cmd != "" {
		return object.Cmd, nil
	}
	return "", errors.New("RabbitMQ pattern must be a string or an object with cmd")
}

func errorPayload(err error) any {
	var application *ApplicationError
	if errors.As(err, &application) {
		return application.Payload
	}
	return map[string]any{"status": "error", "message": err.Error()}
}

func isSideEffecting(pattern string) bool {
	switch pattern {
	case "refund.create", "refund.webhook.xendit.disbursement", "refund.xendit.callback.process", "iluma.checkAccount", "iluma.bankValidator", "refund.iluma.poll":
		return true
	default:
		return false
	}
}

func retryCount(headers amqp.Table) int {
	value, ok := headers["x-retry-count"]
	if !ok {
		return 0
	}
	switch number := value.(type) {
	case int:
		return number
	case int32:
		return int(number)
	case int64:
		return int(number)
	case string:
		result, _ := strconv.Atoi(number)
		return result
	default:
		return 0
	}
}

func cloneHeaders(source amqp.Table) amqp.Table {
	result := make(amqp.Table, len(source)+2)
	for key, value := range source {
		result[key] = value
	}
	return result
}

func stableMessageID(delivery amqp.Delivery) string {
	if value, ok := delivery.Headers["x-idempotency-key"].(string); ok && value != "" {
		return value
	}
	if delivery.MessageId != "" {
		return delivery.MessageId
	}
	digest := sha256.Sum256(delivery.Body)
	return "refund:" + hex.EncodeToString(digest[:])
}

func republish(delivery amqp.Delivery, headers amqp.Table) amqp.Publishing {
	return amqp.Publishing{
		Headers: headers, ContentType: delivery.ContentType, ContentEncoding: delivery.ContentEncoding,
		DeliveryMode: amqp.Persistent, CorrelationId: delivery.CorrelationId, ReplyTo: delivery.ReplyTo,
		MessageId: stableMessageID(delivery), Timestamp: delivery.Timestamp, Type: delivery.Type,
		AppId: delivery.AppId, Body: delivery.Body,
	}
}

func (s *Server) StopAccepting() error {
	if s == nil || s.ch == nil {
		return nil
	}
	var err error
	s.stopOnce.Do(func() { err = s.ch.Cancel(s.consumerTag, false) })
	return err
}

func (s *Server) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		Received: s.received.Load(), Completed: s.completed.Load(), Failed: s.failed.Load(),
		Retried: s.retried.Load(), DeadLettered: s.deadLettered.Load(), RepliesFailed: s.repliesFailed.Load(),
	}
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		if s.ch != nil {
			_ = s.ch.Close()
		}
		if s.conn != nil {
			closeErr = s.conn.Close()
		}
	})
	return closeErr
}
