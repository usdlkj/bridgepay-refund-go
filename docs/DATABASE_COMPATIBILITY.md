# Database and sensitive-data compatibility

Step 9.3 was implemented against the running `kcic_refund` PostgreSQL schema,
which contains later TypeORM synchronization changes not represented by the
oldest Node migration snapshots. Refund-Go does not create, alter, or delete
schema objects at startup.

## Ownership

Refund-Go maps these Node tables:

- `refunds`, `refund_details`, `refund_detail_tickets`, `refund_banks`, and
  `bank_datas`;
- `refund_logs`, `refund_webhook_calls`, `ticketing-call-logs`,
  `iluma_call_logs`, and `iluma_callbacks`;
- `reports`, `report_data_rows`, `configurations`, and `api_log_debugs`.

`payment_gateways` is mapped only so shared-schema compatibility is explicit.
It remains Core-owned and Refund-Go must not write it.

PostgreSQL enum labels are frozen for account state/result, API signature
status, and payment-gateway status. Refund status remains a Node-compatible
varchar with Go constants for all observed values.

## Concurrency and transaction rules

- Initial refund, detail, and ticket inserts use one transaction.
- `refunds.refund_ga_number` is the authoritative duplicate barrier.
  `INSERT ... ON CONFLICT DO NOTHING` converts a race into
  `ErrDuplicateRefund`; the transaction rollback removes partial detail rows.
- A refund must be selected `FOR UPDATE` before a state-changing decision.
- Bank-data creation uses the unique `(bank_code, account_number_hash)` index.
  A competing insert selects the winner `FOR UPDATE` instead of overwriting its
  ciphertext or validation state.
- The store never retries a transaction that may later contain an external
  payout. Higher layers must reconcile ambiguous payout results by persisted
  idempotency evidence.

## Sensitive-data boundary

The encryptor client preserves the Node ClientRMQ patterns `encrypt`,
`decrypt`, and `blind-index`, Nest request/reply envelopes, direct reply-to
correlation, and the exact context/AAD value
`refund.bankData.accountNumber`.

Account numbers are stored only as Encryptor ciphertext plus a blind index.
Generic JSON persistence rejects unmasked values under account-number and
identity-number keys. Ticket identity values must use the full Encryptor
ciphertext JSON shape before being written to the legacy varchar column.
Neither the encryptor client nor the compatibility tests log plaintext,
ciphertext payloads, hashes, provider tokens, private keys, or broker/database
credentials.

## Schema readiness

`/health/readiness` includes `refund_schema`. It fails when a required table or
column is absent, a critical type/enum differs, either duplicate-prevention
index is missing, or the unsafe legacy `bank_datas.account_number` plaintext
column is present. It permits additional columns so additive Node migrations
remain deployable during coexistence.

No provider endpoint is called by these checks or tests.
