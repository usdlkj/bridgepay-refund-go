# Transport compatibility

## Gateway to Refund-Go

Gateway continues to use RabbitMQ during the initial migration. Refund-Go
declares the existing durable `bridgepay-refund` queue with no queue arguments,
matching the Node service and avoiding an inequivalent redeclaration.

The consumer accepts both NestJS pattern forms:

- RPC commands: `{"pattern":{"cmd":"refund.create"},"data":...,"id":"..."}`
- asynchronous work: `{"pattern":"refund.iluma.poll","data":...}`

RPC replies retain the AMQP correlation ID and use NestJS terminal envelopes:
`{"response":...,"isDisposed":true}` or
`{"err":...,"isDisposed":true}`. Consumption uses manual acknowledgements and
bounded prefetch (`RABBITMQ_PREFETCH`, default 16). Reply, retry, and dead-letter
publishes wait for publisher confirmation before the input is acknowledged.
Each handler receives the envelope request ID, AMQP correlation ID, stable
message/idempotency key, and retry count as typed context metadata so later
business steps can persist and reconcile the same identity.

`RABBITMQ_CONSUMER_ENABLED` defaults to `false`. Steps 9.5-9.7 register bank,
account-validation, refund-create/status, and Xendit callback/follow-up
handlers. The Node consumer remains authoritative until explicit cutover; set
the flag to `true` only for an isolated test queue or that cutover.

Gateway-Go now waits for the `refund.webhook.xendit.disbursement` RPC to confirm
that Refund-Go authenticated and persisted the callback before returning
`{message:"OK"}`. The slower balance lookup and Ticketing POST run through the
confirmed `refund.xendit.callback.process` event, so they do not extend the
provider acknowledgement path.

## Replay and background retry policy

No RPC is automatically replayed after its handler starts. In particular,
`refund.create` is acknowledged after its terminal success/error reply even if
the direct reply cannot be delivered. A lost reply must be recovered through
the caller's stable idempotency key and persisted refund/provider evidence;
blind broker replay could create a second payout after Xendit accepted the
first request.

Genuinely asynchronous events, including Xendit callback follow-up, use a
separate durable `<queue>.retry` queue.
The queue applies the configured fixed delay (`RABBITMQ_RETRY_DELAY`, default
60 seconds) and dead-letters back to the main queue. `x-retry-count` is bounded
by `RABBITMQ_MAX_RETRIES` (default 3); exhausted, malformed, and unhandled
events are publisher-confirmed into `<queue>.dlq`. The original Gateway
`x-idempotency-key`/message ID is retained across every attempt.

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
