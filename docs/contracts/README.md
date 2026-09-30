# Golden Contract Fixtures

These fixtures freeze externally visible Node shapes for Go parity tests.
They are synthetic and sanitized: identifiers, signatures, tokens, account
numbers, URLs, and timestamps are non-production examples. They were derived
from the Node DTOs and service return paths without invoking Xendit, Iluma,
Ticketing, or any other external system.

`golden-contracts.json` covers success, pending, duplicate, validation failure,
provider failure, timeout, callback, retry, and terminal outcomes. Each case
contains the transport-level request/body and expected public result or state
effect. `nest-rmq-envelope.json` freezes the Gateway-compatible Nest request
and reply envelopes.

Dynamic fields use explicit sentinels:

- `<RSA_SHA256_SIGNATURE>`: signature bytes encoded by the implementation.
- `<ULID>`: generated internal identifier.
- `<TIMESTAMP>`: generated time.
- `<SANITIZED_MESSAGE>`: error-sanitizer output when provider details vary.

Fixtures are compatibility assertions, not permission to replay a payout.
The `refund.create` case must use a stub provider in automated tests.
