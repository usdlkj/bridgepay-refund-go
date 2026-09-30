// Package encryptor implements the exact NestJS ClientRMQ request/reply
// contract used by the Node refund service. It never logs payloads because
// requests can contain plaintext account or identity data.
package encryptor

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const AccountNumberContext = "refund.bankData.accountNumber"

var ErrRemote = errors.New("encryptor RPC failed")

type Ciphertext struct {
	Enc string         `json:"enc"`
	IV  string         `json:"iv"`
	Tag string         `json:"tag"`
	EDK string         `json:"edk"`
	Alg string         `json:"alg"`
	KMD map[string]any `json:"kmd"`
}

type Client struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string

	publishMu sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]chan reply
}

type reply struct {
	body json.RawMessage
	err  error
}

type rpcEnvelope struct {
	Pattern string `json:"pattern"`
	Data    any    `json:"data"`
	ID      string `json:"id"`
}

type rpcReplyEnvelope struct {
	Err        json.RawMessage `json:"err"`
	Response   json.RawMessage `json:"response"`
	IsDisposed bool            `json:"isDisposed"`
}

func Dial(rawURL, queue string) (*Client, error) {
	conn, err := amqp.Dial(rawURL)
	if err != nil {
		return nil, fmt.Errorf("connect encryptor broker: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open encryptor channel: %w", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare encryptor queue: %w", err)
	}
	deliveries, err := ch.Consume("amq.rabbitmq.reply-to", "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("consume encryptor replies: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("enable encryptor publisher confirms: %w", err)
	}
	client := &Client{conn: conn, ch: ch, queue: queue, pending: make(map[string]chan reply)}
	go client.dispatch(deliveries)
	return client, nil
}

func (c *Client) Encrypt(ctx context.Context, plaintext string) (Ciphertext, error) {
	request := encryptRequest(plaintext)
	var result Ciphertext
	if err := c.call(ctx, "encrypt", request, &result); err != nil {
		return Ciphertext{}, err
	}
	if result.Alg != "AES-256-GCM" || result.Enc == "" || result.IV == "" || result.Tag == "" || result.EDK == "" || result.KMD == nil {
		return Ciphertext{}, errors.New("encryptor returned invalid ciphertext")
	}
	return result, nil
}

func (c *Client) Decrypt(ctx context.Context, payload Ciphertext) (string, error) {
	request := decryptRequest(payload)
	var result string
	if err := c.call(ctx, "decrypt", request, &result); err != nil {
		return "", err
	}
	return result, nil
}

func (c *Client) BlindIndex(ctx context.Context, plaintext string) (string, error) {
	var result string
	if err := c.call(ctx, "blind-index", map[string]any{
		"value": plaintext, "context": AccountNumberContext,
	}, &result); err != nil {
		return "", err
	}
	if result == "" {
		return "", errors.New("encryptor returned an empty blind index")
	}
	return result, nil
}

func encryptRequest(plaintext string) map[string]any {
	return map[string]any{
		"value":   base64.StdEncoding.EncodeToString([]byte(plaintext)),
		"aad":     base64.StdEncoding.EncodeToString([]byte(AccountNumberContext)),
		"context": AccountNumberContext,
	}
}

func decryptRequest(payload Ciphertext) map[string]any {
	return map[string]any{
		"payload": payload,
		"aad":     base64.StdEncoding.EncodeToString([]byte(AccountNumberContext)),
		"context": AccountNumberContext,
	}
}

func (c *Client) call(ctx context.Context, pattern string, data any, target any) error {
	id, err := correlationID()
	if err != nil {
		return err
	}
	body, err := json.Marshal(rpcEnvelope{Pattern: pattern, Data: data, ID: id})
	if err != nil {
		return err
	}
	result := make(chan reply, 1)
	c.pendingMu.Lock()
	c.pending[id] = result
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	c.publishMu.Lock()
	confirmation, err := c.ch.PublishWithDeferredConfirmWithContext(ctx, "", c.queue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: id,
		ReplyTo:       "amq.rabbitmq.reply-to",
		MessageId:     "encryptor:" + id,
		Body:          body,
	})
	if err == nil {
		var acknowledged bool
		acknowledged, err = confirmation.WaitContext(ctx)
		if err == nil && !acknowledged {
			err = errors.New("encryptor request was negatively acknowledged")
		}
	}
	c.publishMu.Unlock()
	if err != nil {
		return err
	}

	select {
	case response := <-result:
		if response.err != nil {
			return response.err
		}
		if err := json.Unmarshal(response.body, target); err != nil {
			return errors.New("encryptor returned an invalid response")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) dispatch(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		var envelope rpcReplyEnvelope
		if json.Unmarshal(delivery.Body, &envelope) != nil || !envelope.IsDisposed {
			continue
		}
		c.pendingMu.Lock()
		result, ok := c.pending[delivery.CorrelationId]
		if ok {
			delete(c.pending, delivery.CorrelationId)
		}
		c.pendingMu.Unlock()
		if !ok {
			continue
		}
		if len(envelope.Err) > 0 && string(envelope.Err) != "null" {
			result <- reply{err: ErrRemote}
			continue
		}
		result <- reply{body: envelope.Response}
	}
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func correlationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
