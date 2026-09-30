# Refund callback and follow-up compatibility

Step 9.7 ports the Xendit payout callback and Ticketing outcome notification
without changing either external endpoint or payload contract. The Node source
remains unchanged.

## Acceptance boundary

Refund-Go retrieves the environment-selected Xendit credential from Core-Go,
requires `X-Callback-Token`, compares it in constant time, validates the frozen
callback fields, and masks account data before persistence. Missing/invalid
tokens are rejected before any callback row or work item is created.

Every authenticated callback delivery is written to `refund_webhook_calls`.
The first delivery for the canonical event identity is the processing row;
later deliveries are stored as duplicate audit rows that point to it. A
PostgreSQL advisory transaction lock makes this durable under concurrent
delivery without adding a table, index, or migration. The event identity uses
Xendit's event, business, payout ID, status, updated time, and failure code, so
pending and terminal callbacks for the same payout remain distinct.

Gateway-Go waits only for this authenticated database write and the
publisher-confirmed background event. It then returns the existing
`{"message":"OK"}` response. Xendit balance retrieval, signing, and Ticketing
delivery happen afterward.

## State transitions

- `accepted`, `requested`, `pending`, and `queued` retain the refund state and
  finish the callback audit row.
- `succeeded` changes `pendingDisbursement|retry` to `success`, stores the
  masked payout result and payout ID, records the refund date, and schedules
  Ticketing follow-up.
- `failed`, `cancelled`, and `reversed` with a terminal Node failure code change
  the refund to `fail` and schedule the signed failure notification.
- Other provider failures compare the existing `retry_attempt` JSON array with
  `REFUND_TRY_COUNT` (default 1). Below the limit they change the refund to
  `fail`, set `retry_date`, and await the existing controlled retry workflow;
  at the limit they send the terminal failure notification.
- Unknown callbacks, callbacks without an eligible refund, completed work, and
  duplicate work do not mutate the refund.

The existing Node configuration-key quirk is retained: retry delay is read
from the key with the leading U+2060 character, defaults to ten minutes, and a
configured value below ten becomes 60 minutes.

## Ticketing follow-up

The success and failure `retData` field order, status wording, masked bank
number, `curType`, fee/rate rules, Jakarta trade time, and RSA signature inputs
match the frozen Node implementation. Refund-Go trusts the stored `notifyUrl`,
as previously decided, and POSTs the same `{retData,signMsg}` JSON. Each attempt
is recorded in `ticketing-call-logs`; an accepted HTTP 200/201 response with
`retCode:0` is also appended to `refunds.notif_log`.

Success notification promotes `success` to `done`. Failure notification keeps
the terminal refund state `fail`. Network errors, balance errors, signing
errors, rejected Ticketing responses, and audit-write errors use the existing
60-second fixed-delay RabbitMQ retry queue for at most three retries. Exhausted
work is marked `ticketing_failed` in the webhook audit response and enters the
existing DLQ; no unsupported refund status is invented.

Ticketing has no idempotency header in the frozen external contract. Delivery
is therefore at-least-once across a process crash at the HTTP boundary; the
durable callback row and per-attempt logs retain reconciliation evidence
without changing that external call.

## Verification

Unit tests cover token rejection, payload validation, masking, durable enqueue,
exact signature ordering, balance and Ticketing HTTP contracts, success
promotion, retry release, and terminal exhaustion. A PostgreSQL integration
test uses the existing Node-owned schema to prove concurrent-safe deduplication,
duplicate audit retention, callback transition, notification logging, and
cleanup. All external-provider tests use loopback fakes; no real Xendit,
Ticketing, or payout request is made.
