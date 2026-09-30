# Parity evidence and cutover runbook

This document closes implementation Step 9.9. It records the evidence that
Refund-Go is compatible with the frozen Node behavior and the controls required
for a later production cutover. It does not authorize or perform that cutover.

## Compatibility matrix

| Boundary | Evidence | Result |
| --- | --- | --- |
| Public and Backoffice HTTP | Router contract tests cover bank list, account validation, refund create/status, Xendit callback acknowledgement, Backoffice, reports, authentication, validation, and error envelopes. | Proven |
| Validation, fees, masking, signing, state, retry, errors | Package tests exercise the frozen DTO rules, amount calculation, account masking, RSA-SHA256 bytes, Iluma/refund/webhook transitions, retry limits, and provider/error normalization. | Proven |
| Node and Go golden behavior | `TestNodeAndGoMatchGoldenRefundFixture` runs the compiled Node helpers and Go code against the same sanitized fixture and synthetic RSA key. It compares validation, amount breakdown, payout/idempotency payload, masked history, status wording, and signature. | Proven |
| Existing PostgreSQL schema | Storage integration tests run against a schema-only copy of `kcic_refund` and cover atomic writes, duplicate barriers, concurrent creates, callbacks, payout evidence, Backoffice retry, and report rows. | Proven |
| Existing encrypted rows | `TestNodeAndGoEncryptedRowsAreMutuallyReadable` has Node create an Encryptor-backed synthetic row for Go to decrypt and has Go create a synthetic row for Node to decrypt. Both rows are removed. | Proven |
| Gateway-to-Refund gRPC | The real Gateway-Go client and Refund-Go server interoperate over all six typed unary RPCs. HTTP contract tests retain validation, status, and response-body compatibility. | Proven locally |
| Broker failures | Live tests cover caller timeout/retry, unroutable publish, application error, background redelivery, retry exhaustion, drain, and DLQ. Side-effecting RPCs are not blindly replayed. | Proven |
| Core credential boundary | The real Refund-Go client and Core-Go server interoperate over the shared typed gRPC credential contract with a five-second call budget. Gateway never receives provider credentials. | Proven locally |
| Iluma, Xendit, Ticketing | Contract tests use loopback fake servers and assert the existing URLs, headers, payloads, status mapping, callback behavior, and retry decisions. | Proven without external calls |
| Reports and scheduled work | Report rows/XLSX and WIB schedule calculation are tested. Scheduler ownership remains disabled by default. | Proven; activation deferred to cutover |

Cross-language and database parity data uses `SYNTHETIC_*` values. Automated
tests must not point provider base URLs at real Iluma, Xendit, Ticketing, or
other external systems and must never initiate a real payout.

## Deployment configuration

Use `.env.example` as the authoritative variable inventory. Inject secrets with
the supported `*_FILE` settings or the deployment secret mechanism; do not put
them in an image or repository. The response-signing private key file is
required in every environment. Before starting an instance, verify:

- the Node-compatible PostgreSQL schema passes `refund_schema` readiness;
- Redis, RabbitMQ, the Encryptor queue, and Core-Go gRPC are reachable;
- the Core-Go client certificate triplet is complete in staging/production;
- Iluma/Xendit credentials and the Xendit callback token are present;
- provider base URLs still match the deployed Node configuration;
- `RABBITMQ_CONSUMER_ENABLED=false` and `REPORT_SCHEDULER_ENABLED=false` until
  their ownership boundaries are deliberately transferred.

Build the existing service binary or container from the reviewed revision. Do
not introduce a schema migration during cutover: Refund-Go uses the existing
Node schema and does not mutate it at startup.

## Queue and scheduler ownership

The durable production queue remains `bridgepay-refund`. At all times there
must be exactly one side-effecting consumer owner and one report scheduler
owner. Node owns both before cutover.

For a consumer handover:

1. Stop routing new refund work briefly and record queue ready/unacknowledged
   counts.
2. Stop the Node consumer and wait for its unacknowledged count to reach zero.
3. Confirm no ambiguous payout remains; reconcile one before proceeding.
4. Start Refund-Go with `RABBITMQ_CONSUMER_ENABLED=true` and confirm exactly
   one consumer on `bridgepay-refund`.
5. Resume routing and watch RPC errors, DLQ depth, refund transitions, and
   callback lag.

Transfer scheduled jobs separately: stop Node scheduling, verify its advisory
lock is released, then enable `REPORT_SCHEDULER_ENABLED=true` on only one
Refund-Go deployment. Multiple replicas may run, but the PostgreSQL advisory
lock must show a single active job leader.

## Incremental cutover order

Use the Step 11 production controls and move one boundary at a time:

1. Deploy Refund-Go with both ownership flags disabled and require readiness.
2. Route read-only bank, status, Backoffice, and report requests; compare
   sanitized results and database reads with Node.
3. Transfer account-validation ownership and observe Iluma latency/retries.
4. Transfer callback/follow-up ownership and observe deduplication, DLQ, and
   Ticketing delivery.
5. Transfer refund creation last, after proving the queue has one consumer and
   the stable order-id idempotency key is present in persisted payout intent.
6. Transfer scheduled-job ownership separately as described above.

Never let Node and Go concurrently execute the same validation, callback,
Ticketing notification, scheduled job, or payout side effect.

## Rollback

Keep the Node deployment and compatible schema available for the agreed
rollback window. To roll back a transferred queue boundary:

1. Stop new routing and disable the Refund-Go consumer.
2. Allow accepted work to drain; record any unacknowledged, retry, and DLQ
   messages plus every in-flight order ID.
3. Reconcile ambiguous payouts before moving messages. Do not replay
   `refund.create` merely because its reply was lost.
4. Start the Node consumer, confirm it is the only queue consumer, then resume
   routing.
5. Disable the Go scheduler before re-enabling the Node scheduler.

Rollback must not reverse compatible database rows. Both implementations use
the same schema and Encryptor format, so the restored owner continues from the
persisted state after reconciliation.

## Ambiguous payout recovery

An ambiguous result means a persisted Xendit payout intent exists but the
service cannot prove whether Xendit accepted the request. Treat this as manual
reconciliation, not an automatic retry:

1. Freeze further create/retry work for the order ID.
2. Read the masked payout intent, result history, provider payout ID, stable
   reference/idempotency key, and logs. Never expose decrypted account data.
3. Query Xendit status using the existing status contract and the persisted
   provider ID or reference evidence.
4. If accepted, persist the provider result and continue the normal callback
   or Ticketing follow-up state machine. If definitively absent, use the
   controlled authenticated retry path with the same idempotency key.
5. If still uncertain, keep the refund in a non-terminal review state and
   escalate; do not issue another payout.

## Verification commands

Run normal checks in Refund-Go, Gateway-Go, and Core-Go:

```sh
go test -race ./...
go vet ./...
```

Run tagged tests only with isolated PostgreSQL and local RabbitMQ/Node paths:

```sh
go test -tags=integration ./internal/storage ./internal/refund ./internal/compat ./internal/rmqserver
```

Gateway's tagged gRPC test provides the live Gateway-to-Refund proof, and
Refund's tagged test provides the Refund-to-Core proof. Temporary databases,
queues, and synthetic rows must
be absent after the run, and the Node consumer/scheduler must remain the only
production owners until a separately approved Step 11 cutover.
