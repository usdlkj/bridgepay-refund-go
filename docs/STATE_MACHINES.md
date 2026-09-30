# Frozen State Machines

This file describes observed Node transitions. A transition not listed here
must not be invented during the initial Go parity port.

## Bank-account validation

States are the pair `account_status/account_result`.

| From | Event | To | Persistence/response |
| --- | --- | --- | --- |
| absent | first account check | `pending/pending` | save blind index, encrypted account, and later Iluma request id |
| `pending/pending` | Iluma completes and account is found, non-VA | `completed/success` | save normalized Iluma data; return signed `success` |
| `pending/pending` | Iluma completes but is not found, is VA, or otherwise invalid | `completed/failed` | save normalized data/failure; return signed `failed` |
| `pending/pending` | bounded synchronous wait expires | `expired/failed` | save timeout diagnostic; return signed `failed`; emit fallback poll |
| `pending/pending` | accepted Iluma callback | `completed/success|failed` | callback is acknowledged `OK` |
| `completed/*` | duplicate callback | unchanged | callback is `Ignored` |
| `expired/*` | callback or worker | unchanged | callback is `Ignored`; worker exits |
| no correlated row | early callback | unchanged | callback is `Ignored` |

Nominal legal transitions in the Node code are `pending -> completed|expired`,
`completed -> completed` for idempotency, and terminal `expired`. The cache TTL
defaults to ten days. Despite the cache intent, the current `checkAccount`
path still submits to Iluma after resolving an existing record; that observed
behavior is a compatibility risk recorded in the main freeze.

## Refund processing

The complete enum is:

`pendingChecking`, `rbdApproval`, `financeApproval`, `pendingDisbursement`,
`retry`, `success`, `done`, `fail`, `reject`, `onHold`, `cancel`.

| From | Event | To | Notes |
| --- | --- | --- | --- |
| absent | create with missing/`1` `ticketCall` | `pendingChecking` | persisted before payout; ticketing precheck is currently mocked |
| absent | create with `ticketCall:0` | `rbdApproval` | persisted before payout |
| initial state | Xendit payout create returns 200 | `pendingDisbursement` | save provider payout id/response; this is the irreversible boundary |
| initial/existing state | non-duplicate create failure | `fail` | save reject reason and refund log if a row exists |
| any existing state | duplicate create | unchanged | application error `retCode:-1` |
| `pendingDisbursement|retry` | callback `accepted|requested|pending|queued` | unchanged | touch update time only |
| `pendingDisbursement|retry` | callback `succeeded` | `success` | save payout response/date and start ticketing notification |
| `success` | ticketing responds HTTP 200/201 with `retCode:0` | `done` | append notification log |
| `success` | ticketing call fails or is not accepted | `success` | no terminal promotion; failed call is logged |
| `pendingDisbursement|retry` | terminal provider failure code | `fail` | notify ticketing failure |
| `pendingDisbursement|retry` | retryable provider failure and limit reached | `fail` | notify ticketing failure |
| `pendingDisbursement|retry` | retryable provider failure below limit | `fail` with `retryDate` | schedule eligibility only; no public retry route exists |
| `fail` | internal backoffice retry method succeeds at Xendit | `retry` | appends retry attempt; method currently has no controller route |

Terminal provider failure codes are `INVALID_DESTINATION`,
`REJECTED_BY_BANK`, `TRANSFER_ERROR`, `EMPTY_ACCOUNT_NAME`, and
`REJECTED_BY_CHANNEL`.

Retry count is read from `REFUND_TRY_COUNT`, default `1`. The comparison is
against the current `retryAttempt` array length. Retry delay nominally comes
from `REFUND_TRY_TIME_PERIOD`, default ten minutes, but the Node lookup contains
an invisible leading character. If the configured/read value is below ten,
the implementation replaces it with 60 minutes.

The enum contains approval, reject, hold, and cancel states used by
administrative/reporting code, but no additional public transition endpoints
were found in the current controller surface. The Go parity implementation
must not infer a new workflow from enum names alone.
