# Backoffice, report, and scheduler compatibility

Step 9.8 ports the Node Refund administrative and report surface without
changing the Node source, external provider endpoints, or Backoffice request
and response handling.

## Service-authenticated HTTP surface

Every route below requires the existing `X-Service-Key` contract:

- `POST /api/v2/refunds` and its trailing-slash form list raw refund entities
  with the frozen six-column filter behavior and joined webhook calls.
- `POST /api/v2/refunds/log` lists raw refund log entities with the frozen
  five-column filters.
- `GET /api/v2/refunds/:id` follows the legacy `refund_details.refund_id`
  association; `GET /api/v2/refunds/refundDetail/:id` follows
  `refunds.refund_detail_id`. Both include ticket rows when a detail exists and
  return JSON `null` when the refund is absent, matching TypeORM behavior.
- `POST /api/v2/refunds/banks/:id` preserves the legacy bank-status adapter in
  addition to `/api/v2/banks/:id`.
- `POST /api/v2/refunds/retry/:refundGANumber` is the controlled route for the
  retry service method that existed in Node but had no controller route. It is
  additive and service-authenticated; existing callers are unchanged.
- `POST /api/v2/report`, `POST /api/v2/report/create`, and
  `GET /api/v2/report/download/:id` preserve report listing, asynchronous
  creation, and XLSX download behavior.

The known Node report-list defect is deliberately visible: non-empty filters
for the frozen indexes that refer to refund-only columns fail instead of being
silently reinterpreted. Report rows continue to live in `reports.report_data`;
the unused `report_data_rows` table is not populated.

## Controlled payout retry

Retry is allowed only from `fail` and is bounded by `REFUND_TRY_COUNT` (default
one). Refund-Go reads the linked encrypted `bank_datas` row and calls the
existing Encryptor `decrypt` RPC only while constructing the provider request.
Plaintext never enters the refund row, audit logs, or request history.

Before the Xendit call, one atomic conditional update records the retry
timestamp, the masked request, and an in-flight `retry` state. The Xendit
reference uses `<refundGANumber>-<attempt>` while the idempotency header remains
the base refund number, exactly as in Node. A provider failure returns the row
to `fail`; a success persists the masked response and payout id. This prevents
two Backoffice replicas from submitting the same retry concurrently and keeps
reconciliation evidence across ambiguous failures.

No test contacts Xendit. Unit tests use a local provider fake and assert that
only the fake receives plaintext while persisted intent is masked.

## Reports and schedules

Refund and Iluma reports preserve the Node field names (including
`refundTrandeNo`, `PlatTradeNo`, `ActualRefundAmount`, and `PassengerName`),
Jakarta formatting, `process` to `completed` transition, JSON persistence, and
XLSX column order. A started report is returned before generation completes.
Interrupted `process` reports are resumed when scheduled ownership is enabled.

The only scheduled work found in the frozen Node service is:

- `0 4 * * *` Asia/Jakarta: previous-day refund report.
- `0 5 * * *` Asia/Jakarta: previous-day Iluma report.

Refund-Go implements those two schedules with PostgreSQL advisory locks and a
same-day type/date existence check so only one replica creates each report.
`REPORT_SCHEDULER_ENABLED=false` is the safe default while Node owns the jobs;
enable it only during the scheduler cutover.

Node has no scheduled bank synchronization or cleanup job. Bank synchronization
therefore remains the already-ported manual authenticated operation from Step
9.5, and no speculative cleanup schedule was added.

## Backoffice routing

After the Step 9.8 HTTP contract suite passed, the Backoffice development
configuration and no-environment fallback were changed from the Node
`refundgateway` target to Refund-Go (`localhost:4000` for development and
`refund-go:4000` for the service-network template). Request bodies, bearer and
service-key headers, response handling, and UI behavior were not changed.

The existing Backoffice Orders Reports UI remains on Core's separate
`/api/report` contract. It is not the Node Refund `/api/v2/report` surface and
was intentionally not rerouted.

## Verification

- Unit tests cover controlled retry, masking, reference/idempotency values,
  report mappings, WIB schedule calculation, XLSX output, service auth, and
  HTTP response/download contracts.
- The full Refund-Go race suite and `go vet ./...` pass.
- All new PostgreSQL selects were checked against the live existing schema.
  Refund retry, report insert/completion, and advisory locking were exercised
  in a transaction that ended with `ROLLBACK`; no synthetic row remained.
- Backoffice TypeScript checking passes after the configuration-only target
  change.
- No real Xendit, Iluma, Ticketing, or Core external call was made, and no
  Docker image was built.
