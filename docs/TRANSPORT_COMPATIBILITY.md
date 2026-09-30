# Transport compatibility

## Gateway to Refund-Go

Gateway-Go uses the typed `bridgepay.refund.v1.RefundGatewayService` gRPC
contract for bank list, account validation, both callback-acceptance paths,
refund creation, and refund status. Gateway keeps one connection, applies an
operation-specific deadline, and never automatically retries a side-effecting
RPC. Refund-Go owns business decisions, persistence, provider calls, signed
response JSON, and idempotency.

The gRPC server repeats security-critical validation, limits request messages
to 1 MiB, publishes standard health, drains accepted work during shutdown, and
requires a Gateway client certificate in staging and production. Gateway
returns Refund-Go's JSON body unchanged at the public HTTP boundary.

Xendit callback success means Refund-Go authenticated and durably recorded the
callback. The slower balance lookup and Ticketing POST remain on the confirmed
`refund.xendit.callback.process` RabbitMQ event, so they do not extend the
provider acknowledgement path.

## Replay and background retry policy

No side-effecting gRPC call is automatically replayed after dispatch. In
particular, a lost `CreateRefund` response must be recovered through
the caller's stable idempotency key and persisted refund/provider evidence;
blind broker replay could create a second payout after Xendit accepted the
first request.

Genuinely asynchronous events, including Iluma polling and Xendit callback follow-up, use a
separate durable `<queue>.retry` queue.
The queue applies the configured fixed delay (`RABBITMQ_RETRY_DELAY`, default
60 seconds) and dead-letters back to the main queue. `x-retry-count` is bounded
by `RABBITMQ_MAX_RETRIES` (default 3); exhausted, malformed, and unhandled
events are publisher-confirmed into `<queue>.dlq`. The original stable
message/idempotency ID is retained across every attempt.

## Refund-Go to Core-Go

Synchronous Core-owned data is accessed through the typed
`bridgepay.refund.v1.RefundCoreService` gRPC contract. Step 9.4 implements
`GetPaymentGatewayCredential`, backed by Core-Go's existing encrypted
payment-gateway service. Refund-Go requests only the `xendit` credential and
preserves Node's environment selection rule: production selects the production
branch; all other environments select development.

The canonical proto is `contracts/refund/v1/core.proto`. Core-Go imports its
shared-module bindings; Refund-Go commits bindings generated from that same
proto under `internal/coreclient/pb` so its Docker build remains self-contained
instead of depending on a path outside the service build context.

Staging and production require a client CA, certificate, and key through the
`CORE_GRPC_TLS_*` settings. Core-Go already requires and verifies client
certificates in those environments. Provider credentials never travel through
Gateway or RabbitMQ. Asynchronous refund work remains RabbitMQ.

The Node Core and Refund implementations are unchanged.
