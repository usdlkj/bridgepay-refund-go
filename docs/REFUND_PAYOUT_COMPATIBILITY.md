# Refund creation and payout-status compatibility

Step 9.6 ports the Node refund creation and status contracts while retaining
Gateway as the validation and forwarding boundary.

## Public and RabbitMQ contracts

- `POST /api/v2/transfer` and RabbitMQ command `refund.create` accept the
  account, invoice, signature, and optional `ticketCall` structure already
  forwarded by Gateway.
- `POST /api/v2/transferQuery` and RabbitMQ command `refund.status` accept the
  order ID and signature structure and return the legacy signed invoice.
- Provider credentials are obtained only from Core-Go through the typed gRPC
  credential service. They are never accepted from or returned to Gateway.
- Responses are signed as RSA-SHA256 over the exact compact JSON invoice
  object before `signMsg` is returned.

## Creation order and persistence

The payout boundary is deliberately ordered:

1. Check the unique `refund_ga_number` and resolve an enabled bank, including
   the legacy `ID_` prefix rule.
2. Reuse or atomically create the account's blind-index/encrypted `bank_datas`
   row together with the refund/detail transaction.
3. Save only masked account and identity values in generic JSON columns.
4. Persist the masked Xendit request intent before making the HTTP request.
   The order ID is both `reference_id` and the idempotency key.
5. Call Xendit, then synchronously persist its masked success or failure result
   before returning to Gateway.

Consequently, a lost or ambiguous provider response leaves durable evidence
with one stable idempotency key. A duplicate client request is rejected before
encryption or another payout call, preventing a second payout.

The currently coded Ticketing lookup remains mocked and therefore performs no
external Ticketing call. Normal requests create the same empty detail record;
`ticketCall: 0` skips it and starts at `rbdApproval` as recorded in the frozen
state machine.

## Xendit contract

- Create: `POST {XENDIT_BASE_URL}/v2/payouts`.
- Status: `GET {XENDIT_BASE_URL}/v2/payouts/{payoutId}`.
- Authentication: Basic authentication using the Core-owned Xendit secret.
- Create headers include JSON content type and `Idempotency-key` equal to the
  order ID.
- The create body preserves `reference_id`, `channel_code`,
  `channel_properties`, amount, description, `IDR`, and the legacy body-level
  `idempotencyKey` field.
- Only HTTP 200 completes creation as `pendingDisbursement`, matching Node.
  Status accepts the provider's normal successful HTTP range.

No Xendit request was made during implementation or verification. Provider
contract tests use loopback HTTP fakes. PostgreSQL tests use synthetic records
and delete them on completion; RabbitMQ verification uses temporary queues
that are also removed.
