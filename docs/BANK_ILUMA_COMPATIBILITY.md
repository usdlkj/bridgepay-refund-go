# Bank Catalogue and Iluma Compatibility

Step 9.5 ports the Node refund service's bank catalogue and bank-account
validation behavior without changing any external provider contract.

## Exposed contracts

- `GET /api/v2/bankCodes` and RabbitMQ command `refund.bankList` return the
  enabled bank catalogue in the legacy `{retCode, retData, retMsg}` envelope.
- `POST /api/v2/checkAccount` and RabbitMQ command `iluma.checkAccount` use the
  same DTO validation, bank resolution, encrypted account storage, polling,
  response signing, and public response shape.
- `POST /api/v2/webhook/iluma/bank-validator` and RabbitMQ command
  `iluma.bankValidator` update only pending validation records. Unknown or
  already-terminal records are ignored. Handler failures are acknowledged as
  `OK` so Iluma does not create callback retry storms.
- `/api/v2/banks/*` retains the Backoffice service-key guard, list filters,
  view/update behavior, and Xendit-then-Iluma synchronization order.

## Frozen provider behavior

- Xendit payout channels use `GET /payouts_channels`, Basic authentication,
  `currency=IDR`, and `channel_category=BANK`. A sync resets every returned
  bank to `disable`, matching Node; an operator must explicitly enable it.
- The Iluma catalogue URL remains
  `https://api.iluma.ai/bank/available_bank_codes`.
- Account submission remains `POST
  {ILUMA_BASE_URL}/v1.2/identity/bank_account_validation_details`; result
  polling appends `/{requestId}`. Both use Basic authentication and the
  configured 10-second default timeout.
- Request logs mask the account number. Plaintext account and identity values
  are never persisted; account numbers use the existing Encryptor ciphertext
  and blind-index contracts.

## Preserved legacy edge behavior

- `BANK_ACCOUNT_CHECK_TTL_DAYS` is evaluated against `last_check_at`, but the
  current Node flow still revalidates a fresh completed record. Refund-Go
  deliberately preserves this behavior for parity.
- Synchronous polling uses bounded exponential backoff. On timeout it first
  makes the row terminal (`expired`/`failed`) and then publishes
  `refund.iluma.poll`. The frozen worker immediately exits for an expired row,
  so the fallback cannot revive it. This known behavior is preserved rather
  than silently redesigning the provider flow during migration.
- Allowed state transitions are `pending -> completed`, `pending -> expired`,
  and idempotent `completed -> completed`; `expired` is terminal.

## Verification boundary

Provider contract tests use local HTTP fakes and assert method, URL, query,
authorization, body, timeout/error normalization, and response handling. No
Xendit or Iluma request was sent while implementing or verifying Step 9.5.
The PostgreSQL statements were also exercised with synthetic rows inside an
explicitly rolled-back transaction, and durable event publication was checked
against the local RabbitMQ container.
