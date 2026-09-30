# Refund inter-service gRPC migration plan

## Goal

Move synchronous communication between Refund-Go and Gateway-Go/Core-Go to
typed gRPC without changing the public HTTP contracts, refund business rules,
database ownership, provider calls, or the Node implementations.

This migration is strictly limited to synchronous request/response
communication. Asynchronous events, delayed work, retries, dead-lettering,
scheduled jobs, and other durable background processing remain on RabbitMQ.

The actual transport replacement is Gateway-Go → Refund-Go. Refund-Go →
Core-Go already uses typed gRPC for payment-gateway credential lookup and needs
hardening and shared-contract verification, not a second implementation.

## Execution status

The local implementation is complete through the transport replacement and
automated interoperability gates:

- the versioned contracts and generated bindings exist in all three Go
  repositories and contract-copy drift is checked by the combined gate;
- Refund-Go exposes all six unary RPCs plus standard health, lifecycle drain,
  request limits, sanitized interceptors, and an mTLS Gateway identity policy;
- Gateway-Go uses one typed Refund connection with explicit deadlines and no
  automatic replay of side-effecting calls;
- Refund-Go's existing Core credential client now enforces the five-second
  deadline per RPC, and real cross-repository wire tests cover both gRPC
  boundaries;
- Gateway's synchronous Refund RabbitMQ client and Refund-Go's six synchronous
  command registrations are removed. RabbitMQ remains for Encryptor RPC and
  durable internal work only.

Production deployment, canary observation, certificate provisioning, and the
rollback-window decision in Step 8 are operational actions and are not claimed
as completed by local tests. They require a separately approved deployment.

## Baseline boundaries before this migration

| Caller | Callee | Current transport | Operations |
| --- | --- | --- | --- |
| Gateway-Go | Refund-Go | RabbitMQ request/reply | bank list, account check, Iluma callback, refund create, refund status, Xendit callback acceptance |
| Refund-Go | Core-Go | typed gRPC | payment-gateway credential lookup |
| Refund-Go | Encryptor | RabbitMQ request/reply | encrypt, decrypt, blind index |
| Refund-Go | Refund-Go background worker | RabbitMQ events | Iluma polling and Xendit callback follow-up |

Only the first two rows are in scope. RabbitMQ remains for Encryptor and
durable Refund-Go background jobs. This plan does not alter calls to Iluma,
Xendit, Ticketing, or any other external system.

Webhook acceptance is included because Gateway waits synchronously for
Refund-Go to authenticate and durably record the callback. Processing after
that acknowledgement remains asynchronous and is not migrated to gRPC.

## Target boundary

```text
Ticketing / provider
  -> Gateway-Go HTTP: authenticate, validate shape, rate limit, correlate
  -> typed mTLS gRPC
  -> Refund-Go: repeat security-critical validation, decide, persist,
                call providers, sign the external response
  -> Gateway-Go: serialize the existing HTTP contract unchanged

Refund-Go
  -> typed mTLS gRPC
  -> Core-Go: return the requested Core-owned credential only
```

Gateway must not calculate fees, interpret refund states, select providers,
access Refund tables, or retry a side-effecting RPC automatically. Refund-Go
remains the sole owner of refund decisions and persistence. Core-Go remains the
sole owner of payment-gateway credentials.

## Contract

Add `bridgepay.refund.v1.RefundGatewayService` with these unary RPCs:

1. `ListBanks(ListBanksRequest) returns (RefundHTTPResponse)`
2. `CheckAccount(CheckAccountRequest) returns (RefundHTTPResponse)`
3. `AcceptIlumaCallback(AcceptIlumaCallbackRequest) returns (RefundHTTPResponse)`
4. `CreateRefund(CreateRefundRequest) returns (RefundHTTPResponse)`
5. `GetRefundStatus(GetRefundStatusRequest) returns (RefundHTTPResponse)`
6. `AcceptXenditCallback(AcceptXenditCallbackRequest) returns (RefundHTTPResponse)`

Every request carries a `RequestContext` containing `request_id` and
`idempotency_key`. There is only one merchant, so the contract must not add
merchant routing or speculative multi-tenant fields.

Use typed messages for stable business fields. Preserve raw provider callback
bodies as `bytes` where exact input is required for authentication, audit, or
future-compatible parsing. Pass the Xendit callback token as an explicit
sensitive request field, not generic gRPC metadata or a loggable map.

Refund responses must expose the same `retCode`, `retMsg`, response data, and
`signMsg` needed by the existing HTTP contract. Gateway performs serialization
only; it must not recreate signed payloads or business wording.

Keep the existing `bridgepay.refund.v1.RefundCoreService` and
`GetPaymentGatewayCredential` RPC. Do not add Core RPCs without a real current
caller.

## Error and retry rules

Map failures consistently:

| Condition | gRPC status | Gateway HTTP behavior |
| --- | --- | --- |
| malformed request | `InvalidArgument` | existing validation response |
| invalid callback authentication | `Unauthenticated` | existing webhook rejection |
| missing refund | `NotFound` | existing refund-not-found response |
| duplicate/conflicting request | `AlreadyExists` or typed business result | existing compatibility response |
| illegal lifecycle transition | `FailedPrecondition` | existing compatibility response |
| dependency unavailable before side effects | `Unavailable` | 503 |
| deadline exceeded | `DeadlineExceeded` | existing 503 compatibility response |
| unexpected failure | `Internal` | sanitized 500 response |

Expected business outcomes that already return HTTP 200 with a signed body
remain successful RPC responses, not transport errors.

The initial client performs no automatic RPC retries. Gateway must never
automatically retry `CreateRefund` or either callback RPC after dispatch:
the server may have committed or reached Xendit before the connection failed.
Recovery uses the stable idempotency key and `GetRefundStatus`.

Initial deadlines:

- bank list and refund status: 5 seconds;
- account check: 10 seconds, covering the current bounded Iluma polling window;
- refund creation: 30 seconds;
- callback durable acceptance: 10 seconds;
- Refund-Go credential lookup from Core-Go: 5 seconds.

These are maximum request budgets, not additional retries.

## Implementation steps

### 1. Freeze transport behavior

- Capture golden fixtures for all six Gateway→Refund commands, including
  successful bodies, business failures, validation failures, callback headers,
  deadlines, and duplicate requests.
- Record the corresponding public HTTP status, headers, JSON, signature, and
  database effects.
- Confirm that no Core↔Refund RabbitMQ operation exists. Treat any later
  discovery as a separate reviewed scope change.

**Complete when:** fixtures describe every current Gateway→Refund request and
response without calling a real provider.

### 2. Add and version the protobuf contract

- Add the Refund Gateway service under `contracts/refund/v1`.
- Generate bindings for Gateway-Go and Refund-Go using the existing repository-
  local contract approach.
- Keep Core-Go's credential RPC in the same versioned refund package, but do
  not make Core depend on Gateway-specific handlers.
- Extend the combined verification script to reject drift between the contract
  sources and generated bindings used by all participating repositories.
- Document backward-compatible protobuf evolution rules: never reuse field
  numbers, reserve removed fields, and add rather than rename fields.

**Complete when:** all three repositories compile from clean checkouts and a
contract-drift test fails if their required contract copies diverge.

### 3. Implement the Refund-Go gRPC server adapter

- Add a gRPC server on a separate address, default `:50052`.
- Register `grpc.health.v1.Health` and `RefundGatewayService`.
- Make each RPC a thin adapter over the existing bank, Iluma, refund, and
  webhook services. Do not duplicate business logic from RabbitMQ handlers.
- Move shared decode/validate/error mapping out of the RabbitMQ registration
  function only where needed so both adapters call the same code.
- Propagate request IDs into logs and lifecycle tracking. Redact callback
  tokens, account numbers, identity numbers, credentials, and raw provider
  payloads.
- For callbacks, return success only after authentication and durable database
  acceptance. Keep slower follow-up work on Refund-Go's durable background
  queue.
- Stop accepting new RPCs during shutdown, then drain the existing lifecycle
  tracker before exiting.

**Complete when:** in-process and wire tests prove every RPC calls the existing
business service once and preserves its result and durable effects.

### 4. Secure Refund-Go's gRPC server

- Add `REFUND_GRPC_LISTEN_ADDRESS`, server certificate, server key, and client
  CA configuration.
- Require mutual TLS in staging and production. Development may use plaintext
  only when explicitly configured for local use.
- Accept only the Gateway client identity on `RefundGatewayService`.
- Apply maximum message sizes no larger than the current 1 MiB Gateway HTTP
  body limit.
- Add unary interceptors for request correlation, deadline enforcement,
  sanitized access logs, panic recovery, and status/latency metrics.

**Complete when:** tests reject missing/untrusted client certificates and prove
sensitive request fields never appear in logs.

### 5. Add the Gateway-Go Refund gRPC client

- Add `REFUND_GRPC_ADDRESS` and `REFUND_GRPC_TLS_*` settings parallel to the
  existing Core client settings.
- Implement one long-lived connection and typed client; do not dial per
  request.
- Replace the Refund service's string command dispatcher with the typed client.
- Keep all existing HTTP routes, middleware, validation, response bodies, and
  signatures unchanged.
- Update readiness to use the standard Refund gRPC health service.
- Map gRPC statuses to the existing sanitized HTTP errors.
- Apply explicit per-operation deadlines and the retry rules above.

**Complete when:** Gateway HTTP contract tests pass unchanged through a real
Refund-Go gRPC server.

### 6. Harden the existing Refund-Go → Core-Go gRPC client

- Retain the current typed credential call and five-second deadline.
- Verify Core-Go requires the Refund client certificate in staging and
  production and exposes only the requested credential envelope.
- Add standard Core gRPC health checking if available; otherwise retain the
  typed credential wire test and TCP readiness check until health is added.
- Add a cross-repository test using Refund-Go's real client and Core-Go's real
  server, including not-found, invalid request, unavailable, and mTLS cases.
- Do not cache decrypted credentials in Gateway or persist them in Refund-Go.

**Complete when:** Refund-Go's only Core-owned dependency is proven over typed
mTLS gRPC and no credential traverses Gateway or RabbitMQ.

### 7. Prove behavioral parity and failure safety

- Run the same sanitized fixtures through the RabbitMQ and gRPC adapters and
  compare responses, signatures, database rows, and emitted background jobs.
- Cover bank list, account validation, both callbacks, refund creation, status,
  duplicate creation, invalid signatures/tokens, expired deadlines, server
  shutdown, and lost responses after durable commit.
- Assert that a lost `CreateRefund` response never causes an automatic second
  payout attempt.
- Use fake Iluma, Xendit, Ticketing, and Core servers. Never call a real
  provider or initiate a real payout.
- Extend the clean-checkout quality gate to run Gateway→Refund and Refund→Core
  gRPC interoperability tests.

**Complete when:** the gRPC path is byte-compatible at the public HTTP boundary
and all failure-path tests pass in clean checkouts.

### 8. Cut over without dual side effects

- Deploy Refund-Go with the gRPC server enabled while Gateway still uses
  RabbitMQ.
- Add a temporary Gateway setting `REFUND_TRANSPORT=rabbitmq|grpc`, defaulting
  to `rabbitmq` until verification is complete.
- Exercise read-only RPCs in staging. Shadow comparison may call only bank list
  and status; never shadow account validation, create, or callbacks because
  they persist state or call providers.
- Switch one Gateway canary to gRPC. Each incoming request uses exactly one
  transport; never send the same request through both.
- Monitor error rate, latency, duplicate barriers, payout attempts, callback
  lag, and ambiguous results before switching all Gateway instances.
- Roll back by switching Gateway to RabbitMQ. No database rollback is needed.

**Complete when:** all Gateway instances use gRPC, metrics remain within the
agreed window, and rollback has been rehearsed without duplicate side effects.

### 9. Retire only the inter-service RabbitMQ RPC path

- Drain outstanding replies and confirm Gateway has no RabbitMQ refund RPCs in
  flight.
- Disable and then remove registration of the six RabbitMQ command handlers.
- Remove Gateway's Refund RabbitMQ client, queue configuration, readiness
  check, and request/reply tests.
- Keep Refund-Go's RabbitMQ background-event consumer and Encryptor client.
  If necessary, give internal jobs a dedicated queue before removing the old
  public RPC queue; do not replace durable jobs with best-effort goroutines.
- Remove the temporary transport flag after the rollback window.
- Update the compatibility matrix and operational documentation.

**Complete when:** no Gateway↔Refund or Core↔Refund request/reply traffic uses
RabbitMQ, while durable internal jobs and Encryptor messaging still function.

## Acceptance criteria

- Public Gateway HTTP contracts and signatures are unchanged.
- Gateway contains transport validation and serialization only; refund business
  decisions remain in Refund-Go.
- Refund-Go communicates with both Gateway-Go and Core-Go through typed gRPC.
- Staging and production use mTLS with distinct Gateway and Refund identities.
- Side-effecting RPCs are never automatically replayed.
- Callback success means the callback is durably accepted before acknowledgement.
- Clean-checkout unit, race, contract, database, and cross-repository gRPC tests
  pass without real external calls.
- The Node Gateway, Core, and Refund implementations are untouched.

## Deliberately deferred

- Moving Refund-Go's Encryptor calls from RabbitMQ.
- Replacing Refund-Go's durable internal background queue.
- Moving any asynchronous event, retry, dead-letter, or scheduled-job flow to
  gRPC.
- Adding speculative Core↔Refund operations with no current caller.
- Removing the RabbitMQ rollback path before the monitored cutover window ends.
