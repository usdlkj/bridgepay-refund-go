package rmqserver

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPatternNameSupportsNestCommandAndEventShapes(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{`{"cmd":"refund.create"}`, "refund.create"},
		{`"refund.iluma.poll"`, "refund.iluma.poll"},
	}
	for _, test := range tests {
		got, err := patternName([]byte(test.raw))
		if err != nil || got != test.want {
			t.Fatalf("patternName(%s) = %q, %v; want %q", test.raw, got, err, test.want)
		}
	}
}

func TestApplicationErrorPreservesPayload(t *testing.T) {
	payload := map[string]any{"statusCode": 422, "message": "invalid refund"}
	got := errorPayload(&ApplicationError{Payload: payload})
	result, ok := got.(map[string]any)
	if !ok || result["statusCode"] != 422 || result["message"] != "invalid refund" {
		t.Fatalf("errorPayload() = %#v", got)
	}
	ordinary := errorPayload(errors.New("failed")).(map[string]any)
	if ordinary["status"] != "error" || ordinary["message"] != "failed" {
		t.Fatalf("ordinary payload = %#v", ordinary)
	}
}

func TestStableMessageIDPrefersGatewayIdempotencyKey(t *testing.T) {
	delivery := amqp.Delivery{
		Headers:   amqp.Table{"x-idempotency-key": "refund.create:stable"},
		MessageId: "fallback",
		Body:      []byte("body"),
	}
	if got := stableMessageID(delivery); got != "refund.create:stable" {
		t.Fatalf("stableMessageID() = %q", got)
	}
}

func TestSideEffectingPatternsIncludeRefundCreateAndCallback(t *testing.T) {
	if !isSideEffecting("refund.create") || !isSideEffecting("refund.xendit.callback.process") || isSideEffecting("refund.status") {
		t.Fatal("side-effect classification is unsafe")
	}
}
