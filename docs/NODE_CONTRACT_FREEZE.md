# Node Refund Contract Freeze

Status: frozen for Step 9.1 on 2026-09-28.

Node source baseline: commit `445220a1641d3412f209b304aa5e58de6a44fd7c`
with a clean working tree at inspection time.

This document is the compatibility baseline for migrating
`bridgepay-refund` to `bridgepay-refund-go`. It records behavior observed in
the Node source at `06_refund-core/bridgepay-refund`; it is not a redesign.
The Node source was not changed and no external provider was called while
creating this freeze.

## Ownership boundary

| Concern | Owner | Boundary |
| --- | --- | --- |
| Ticketing-facing refund and account-check HTTP validation | Gateway | Gateway accepts the public request and forwards the compatible RabbitMQ request/reply payload. |
| Refund decisions and persistence | Refund service | Owns refund state, fee calculation, bank mapping, account-validation state, payout orchestration, provider callbacks, ticketing notification, reports, and refund audit records. |
| Payment-provider credentials | Core | Refund requests Xendit credentials using `get-credential-by-pg-code`; it must not own a second credential source. |
| Sensitive bank-account cryptography | Encryptor | Refund calls `blind-index` and `encrypt` with context/AAD `refund.bankData.accountNumber`; plaintext must not be logged or stored. |
| Refund administration | Backoffice + Refund | Backoffice is the caller/UI. Refund owns the guarded administration and report APIs. |
| External payout | Xendit | Creating a payout is an irreversible boundary. An ambiguous response must be reconciled by idempotency key and provider status, never blindly replayed. |
| Bank-account validation | Iluma | Refund submits and polls validation and accepts callbacks, while persisting the correlation and normalized result. |
| Refund outcome notification | Ticketing | Refund POSTs the result to the request's `notifyUrl`; this URL is trusted by the current product decision. |

The initial Go migration retains Gateway-to-Refund RabbitMQ compatibility.
Moving this boundary to gRPC is a separate change. Synchronous Core-owned
lookups may use typed Core-Go gRPC in a later implementation step.

## HTTP surface

Global behavior: the Node service uses Nest's validation pipe with transform
and whitelist enabled, but unknown properties are stripped rather than
rejected. The default HTTP port is 4000. Backoffice routes require an exact,
constant-time `X-Service-Key` match.

| Method and route | Input | Result/side effect | Execution |
| --- | --- | --- | --- |
| `GET /` | none | `Hello World!` | synchronous |
| `POST /api/v2/checkAccount` | `CheckAccountDto` | signed account result or normalized error; reads/writes bank data and calls Iluma | synchronous, bounded poll plus queued fallback |
| `POST /api/v2/webhook/iluma/bank-validator` | provider JSON | `{message:"OK"}` or `{message:"Ignored"}`; updates callback and bank data | synchronous acknowledgement |
| `GET /api/v2/bankCodes` | none | enabled banks with Iluma mapping | synchronous |
| `POST /api/v2/transfer` | `CreateRefundDto` | signed refund response or error; persists refund and can create Xendit payout | synchronous and irreversible |
| `POST /api/v2/transferQuery` | `StatusRefundDto` | signed status; may query Xendit payout | synchronous |
| `POST /api/v2/webhook/xendit/disbursement` | Xendit callback + `x-callback-token` | immediately returns `{message:"OK"}` and starts processing without awaiting it | background after HTTP acknowledgement |
| `POST /api/v2/refunds/banks/:id` | `{bankStatus:"enable"|"disable"}` | updates bank status | synchronous, guarded |
| `POST /api/v2/refunds/` | `{query?: column[]}` | refund DataTables result | synchronous, guarded |
| `POST /api/v2/refunds/log` | `{query?: column[]}` | refund-log DataTables result | synchronous, guarded |
| `GET /api/v2/refunds/refundDetail/:id` | path id | refund detail | synchronous, guarded |
| `GET /api/v2/refunds/:id` | path id | refund view | synchronous, guarded |
| `POST /api/v2/banks/sync` | none | synchronizes Xendit and Iluma bank catalogues | synchronous, guarded, external calls |
| `POST /api/v2/banks/` | `{query?: column[]}` | bank DataTables result | synchronous, guarded |
| `GET /api/v2/banks/:id` | path id | bank view | synchronous, guarded |
| `POST /api/v2/banks/:id` | `UpdateRefundBankDto` | updates only status/deletion fields | synchronous, guarded |
| `POST /api/v2/report/` | `{query?: column[]}` | report DataTables result | synchronous, guarded |
| `POST /api/v2/report/create` | `{date,type}` | starts report generation without awaiting completion | background, guarded |
| `GET /api/v2/report/download/:id` | path id | XLSX or HTTP 500 | synchronous, guarded |

Backoffice filter-index bounds are part of the contract: refund list `0..5`,
refund logs `0..4`, banks `0..2`, and reports `0..5`. Report type is `refund`
or `iluma`. Refund XLSX headers retain their current spelling and case,
including `refundTrandeNo`, `PlatTradeNo`, and `ActualRefundAmount`.

## RabbitMQ surface

The durable queue is `bridgepay-refund`. Current Node startup uses Nest RMQ
with `noAck: true`. RPC callers use the Nest envelope
`{pattern,data,id}` and expect the Nest response/error envelope on the reply
queue. The Go port must preserve this wire shape before improving delivery
internals.

| Direction | Pattern | Data | Semantics |
| --- | --- | --- | --- |
| inbound RPC | `{cmd:"iluma.checkAccount"}` | `CheckAccountDto` | same behavior as HTTP account check |
| inbound RPC | `{cmd:"iluma.bankValidator"}` | callback object | same behavior as Iluma webhook |
| inbound RPC | `{cmd:"refund.bankList"}` | empty/ignored | enabled bank catalogue |
| inbound RPC | `{cmd:"refund.create"}` | `CreateRefundDto` | creates refund and may create payout |
| inbound RPC | `{cmd:"refund.status"}` | `StatusRefundDto` | returns signed refund status |
| inbound RPC | `{cmd:"refund.webhook.xendit.disbursement"}` | `{payload,headers,req}` | validates and processes Xendit callback before reply |
| self event | `"refund.iluma.poll"` | `{requestId,bankDataId}` | fire-and-forget fallback poll |
| outbound RPC to Core | `"get-credential-by-pg-code"` | `{pgCode:"xendit"}` | obtains environment-specific Xendit secret and callback token |
| outbound RPC to Encryptor | `"blind-index"` | `{value,context:"refund.bankData.accountNumber"}` | stable lookup token |
| outbound RPC to Encryptor | `"encrypt"` | `{value:base64(account),aad:base64(context),context}` | encrypted account storage |

## DTO and response freeze

`CheckAccountDto` requires non-empty string `signMsg` and
`reqData.account.{bankId,accountNo,accountType,idNo,idType,name}`.

`CreateRefundDto` requires non-empty string `signMsg`, all account fields,
`idType` in `1|2|3`, numeric `refundAmount`, and non-empty
`invoice.{orderId,reason,passengers,originalOrderNumber,notifyUrl,ticketOffice}`.
Optional numeric `ticketCall` is interpreted as `0` to skip the ticketing
precheck and `1`/missing to run it.

`StatusRefundDto` requires non-empty `signMsg` and
`reqData.invoice.orderId`. Xendit callbacks require event/business/created and
data id, numeric amount, channel code, currency, status, reference id,
created/updated, and `channel_properties.account_number`.

Public business responses use `retCode`, `retMsg` or `message`, optional
`retData`, and for successful signed responses `signMsg`. Validation failures
use Nest's standard HTTP 400 body on HTTP routes. RPC service failures are
thrown as `RpcException` payloads. Exact sanitized examples live under
`docs/contracts/fixtures`.

Important response rules:

- Account check can return HTTP/RPC success with `retCode: 0` and
  `retData.status` equal to `success` or `failed`.
- A normalized Iluma/provider problem can return `retCode: -1` with an
  `errorCode`; several such HTTP responses deliberately use HTTP 200.
- Refund creation returns invoice status wording for
  `pendingDisbursement` after Xendit accepts the payout.
- Duplicate creation is an application error with `retCode: -1`; HTTP maps it
  from conflict handling while RabbitMQ carries it as an RPC error.
- Refund status includes `curType:"360"`, `pgCode:"xendit"`, internal
  `mwNo`, amount/status, optional masked bank number/comment/trade time, and a
  signature over `{invoice}`.
- Xendit HTTP webhook acknowledgement does not prove background processing
  succeeded.
- Refund, refund-log, bank, and report list APIs return raw JSON arrays of
  TypeORM entities; they do not wrap the result in a pagination envelope.
- Refund/bank detail and bank update return a raw entity or `null` where the
  current service allows it. Missing bank view/update returns HTTP 404 body
  `{status:404,message:"Refund bank not found"}`.
- Bank synchronization returns `{status:200,message:"Success"}`. Most
  backoffice/database failures return the Nest HTTP status with body
  `{status:500,message:<message>}`.
- Manual report creation returns the newly saved raw report entity while its
  background generator is still running. The download helper returns
  `{status:200,title,type,data}` internally; the controller translates that
  into XLSX, or raises HTTP 500 on `{status:500,msg}`.
- Missing, wrong, or unconfigured `X-Service-Key` is a Nest HTTP 401 error
  with the corresponding message (`Missing X-Service-Key header`, `Invalid
  service key`, or `Service-to-service authentication not configured`).

## Persistence and mutation inventory

The Go implementation must use the existing PostgreSQL schema in place.

| Table/entity | Owned mutations |
| --- | --- |
| `refunds` | refund identity, status, amount/fee data, masked request/bank data, retry schedule/history, payout id/response, ticketing notification log, rejection and approval metadata |
| `refund_details`, `refund_detail_tickets` | ticketing/refund detail and passenger/ticket rows |
| `refund_banks` | Xendit/Iluma mapping, provider snapshots, enable/disable, soft delete |
| `bank_datas` | unique `(bank_code,account_number_hash)`, encrypted account, Iluma request/result/failure and timestamps |
| `refund_logs` | API/disbursement failure audit |
| `refund_webhook_calls` | sanitized provider callbacks, including unknown refund references |
| `ticketing-call-logs` | ticketing request/response audit |
| `iluma_call_logs`, `iluma_callbacks` | provider request, polling, and callback audit/correlation |
| `reports` | report request, process/completed state, and current JSON report rows |
| `report_data_rows` | schema exists; current Node report save path is commented out and does not populate it |
| `configurations` | reads retry count/period and operational values |
| `api_log_debugs` | request/signature diagnostics where invoked |

Refund creation uses a database transaction for initial creation and relies on
the unique `refund_ga_number` constraint for duplicate protection. Sensitive
values must remain encrypted/masked. Xendit webhook processing records a
sanitized callback even if its reference does not match an eligible refund.

## Scheduled work and reports

- `0 4 * * *` Asia/Jakarta: create the previous day's refund report.
- `0 5 * * *` Asia/Jakarta: create the previous day's Iluma report.
- Manual report creation returns before generation finishes.
- Reports transition from processing to completed and are downloaded as XLSX.

## External call freeze

| System | Operation | Frozen behavior |
| --- | --- | --- |
| Xendit | `POST https://api.xendit.co/v2/payouts` | Basic secret; `Idempotency-key` is refund order id, with retry suffix where applicable; sends reference, channel/account/name, amount, IDR, description. Irreversible/ambiguous side-effect boundary. |
| Xendit | `GET https://api.xendit.co/v2/payouts/{id}` | used by refund status |
| Xendit | `GET https://api.xendit.co/balance` | used while building callback notification |
| Xendit SDK | payout-channel listing | used by bank synchronization |
| Iluma | `POST {ILUMA_BASE_URL}/bank_account_validation_details` | Basic token; submits validation |
| Iluma | `GET {ILUMA_BASE_URL}/bank_account_validation_details/{id}` | bounded polling and worker fallback |
| Iluma | `GET https://api.iluma.ai/bank/available_bank_codes` | used by bank synchronization/legacy adapter |
| Ticketing | `POST {invoice.notifyUrl}` | sends signed success/failure outcome; current decision trusts supplied URL |
| Core | credential lookup | environment-selects Xendit secret/callback token |
| Encryptor | blind-index/encrypt | exact context and AAD noted above |

## Known compatibility risks (freeze, do not silently fix)

1. The intended ticketing pre-refund lookup is commented out and
   `mockedTicketingCheck` currently supplies the result. The Go port must make
   any change to this behavior explicit and separately approved.
2. Account-check resolution does not currently short-circuit after finding a
   fresh completed cache entry; it still calls Iluma.
3. A synchronous Iluma timeout marks bank data expired and emits a fallback
   poll, while the worker ignores non-pending rows. The fallback therefore
   cannot revive that expired row.
4. The retry-period configuration lookup contains an invisible leading
   character in the key `REFUND_TRY_TIME_PERIOD`; observed defaults may be used
   even when a visually identical configuration exists.
5. Backoffice retry-disbursement logic exists in the service but no controller
   route exposes it.
6. Report rows are stored in `reports.report_data`; persistence to
   `report_data_rows` is currently commented out.
7. The HTTP Xendit webhook intentionally acknowledges before callback-token
   validation and processing complete. The RPC handler does wait.

These are parity-test cases, not endorsements of the behavior.
