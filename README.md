# bridgepay-refund-go

Go replacement for `bridgepay-refund`. The migration is incremental: the Node
service remains the behavioral reference and rollback target until the parity
plan is complete.

Steps 9.2-9.8 provide the operational shell, compatible persistence layer,
typed gRPC boundaries, RabbitMQ background worker, bank catalogue, and Iluma account
validation flow, refund creation and payout-status handling, and durable Xendit
callback/Ticketing follow-up processing, Backoffice operations, reports, and
cutover-gated scheduled work.

## Run locally

1. Copy `.env.example` to `.env` and provide a local PostgreSQL database plus
   an absolute path to the required response-signing private key.
2. Ensure Redis, RabbitMQ, Core-Go, and the encryptor queue are reachable.
3. Run `make run`, or set `REFUND_PRIVATE_KEY_FILE` and use
   `docker compose -f docker-compose.local.yml up --build`.

Do not commit `.env`, private keys, provider tokens, or service-to-service
secrets. Staging and production require the Iluma and service-to-service
secrets; mounted `*_FILE` values are supported.

## Operational endpoints

- `GET /health/liveness`: process liveness only.
- `GET /health/readiness`: PostgreSQL, Redis, RabbitMQ, encryptor queue, Core
  transport, Iluma, and Xendit readiness.
- `GET /health`: alias for readiness.
- `GET /api/v2/bankCodes`: enabled refund-bank catalogue.
- `POST /api/v2/checkAccount`: Iluma-backed account validation.
- `POST /api/v2/webhook/iluma/bank-validator`: Iluma callback acknowledgement.
- `POST /api/v2/transfer`: refund creation and Xendit payout submission.
- `POST /api/v2/transferQuery`: persisted refund and Xendit payout status.
- `POST /api/v2/webhook/xendit/disbursement`: authenticated, durably recorded
  Xendit payout callback; provider and Ticketing follow-up runs asynchronously.
- `/api/v2/banks/*`: service-key-protected Backoffice bank operations.
- `/api/v2/refunds/*`: service-key-protected refund list, detail, logs, legacy
  bank status, and controlled payout retry.
- `/api/v2/report/*`: service-key-protected refund/Iluma report creation,
  listing, and XLSX download.

Provider readiness sends an unauthenticated `HEAD` request and treats any
non-5xx HTTP response as reachable. It never includes credentials or response
bodies. Set `EXTERNAL_HEALTH_CHECKS=false` only in an isolated local/test
environment; the checks are then reported as skipped. Core readiness proves TCP
reachability because Core-Go does not expose the standard gRPC health service;
the typed credential RPC is exercised by transport tests.

Every HTTP response carries `X-Request-ID`, and request logs are structured
JSON. SIGINT/SIGTERM marks readiness as draining, rejects new non-liveness
work, drains accepted operations within `SHUTDOWN_TIMEOUT`, and reports any
unfinished side-effecting operation without logging its payload.

## Verification

Run `make check` to verify formatting, vetting, tests, and the production
binary build.

`make test-integration` additionally runs the tagged database and
cross-language tests when the documented `TEST_*` database, RabbitMQ, and Node
repository variables are supplied. These tests use only `SYNTHETIC_*` records
and remove them before returning.

Contract and ownership documentation is in `docs/`.

The Refund gRPC bindings are generated from the repository-local contract copy
with `make generate-contracts` and committed so clean checkouts and Docker
builds remain self-contained. Cross-repository verification rejects contract
drift.

Database coexistence, transaction rules, schema readiness, and sensitive-data
handling are documented in `docs/DATABASE_COMPATIBILITY.md`.

gRPC request/response compatibility, RabbitMQ background retry/DLQ behavior,
the no-replay rule for payout operations, cutover controls, and the typed Core-Go boundary are
documented in `docs/TRANSPORT_COMPATIBILITY.md`.

The staged plan for moving Gateway-Go ↔ Refund-Go request/reply traffic to
gRPC and hardening the existing Refund-Go → Core-Go gRPC boundary is in
`docs/GRPC_COMMUNICATION_MIGRATION_PLAN.md`.

Bank catalogue, account validation, provider compatibility, and deliberately
preserved Node edge behavior are documented in
`docs/BANK_ILUMA_COMPATIBILITY.md`.

Refund creation ordering, durable idempotency evidence, sensitive-data rules,
and Xendit create/status compatibility are documented in
`docs/REFUND_PAYOUT_COMPATIBILITY.md`.

Xendit callback deduplication, refund transitions, Ticketing signatures, and
retry/terminal behavior are documented in
`docs/REFUND_WEBHOOK_COMPATIBILITY.md`.

Backoffice routes, controlled retry, report/XLSX behavior, WIB schedules,
distributed locking, and cutover controls are documented in
`docs/BACKOFFICE_REPORT_COMPATIBILITY.md`.

The consolidated Step 9.9 compatibility matrix, deployment prerequisites,
queue/scheduler ownership, rollback, and ambiguous-payout recovery procedure
are documented in `docs/PARITY_AND_CUTOVER.md`.
