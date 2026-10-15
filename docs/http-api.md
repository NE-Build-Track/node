# HTTP API

Routes are unversioned. Orchestrator routes support a deployment base path;
signer routes are served at the listener root. A proxy exposing the signer under
a base path must strip that prefix before forwarding requests.
See [orchestrator setup](orchestrator.md) and [signer setup](signer.md) for
configuration.

## Live Runner HTTP

| Method and route | Purpose | Auth / envelope |
| --- | --- | --- |
| `POST /runners/heartbeat` | Register or reconcile dynamic runner | `Authorization` bootstrap credential on first call, derived heartbeat credential thereafter; JSON `runner_id`, interval, TTL, optional new credential, `session_ids`, optional `o2r`. |
| `POST /runners/{runner_id}/unregister` | Remove dynamic runner | Scoped heartbeat credential. |
| `GET /discovery` | Find available runners | JSON discovery entries with app, URL, mode, capacity and price. |
| `POST /apps/{runner_id}/session` | Reserve persistent session | JSON `session_id`, `app_url`, `control_url`; paid mode may first return a payment challenge. |
| `POST /apps/{runner_id}/session/{session_id}/stop` | Stop client session | Session authorization and callback reconciliation. |
| `POST /apps/{runner_id}/session/{session_id}/payment` | Submit a ticket payment | On-chain only. |
| `POST /refresh-payment` | Refresh ticket parameters for the same payer and manifest | On-chain only; JSON `sender` and `manifest_id`. |
| `* /apps/{runner_id}/session/{session_id}/app/{app_path...}` | Persistent proxy | Preserve HTTP method, streaming, SSE and WebSocket semantics and Livepeer session headers. |
| `* /apps/{runner_id}/app/{app_path...}` | Single-shot proxy | Transient capacity-limited reservation; identical session/control headers and cancellation ownership. |
| `POST /runner/{runner_id}/session/{session_id}/channels` | Create generic trickle channels | Scoped session callback token; JSON `channels`. |
| `DELETE /runner/{runner_id}/session/{session_id}/channels` | Delete generic trickle channels | Scoped session callback token. |
| `POST /runner/{runner_id}/session/{session_id}/proxy` | Create session proxy URL | Scoped session callback token; JSON `target_url`; destination policy applies. |
| `POST /runner/{runner_id}/session/{session_id}/stop` | Runner callback to release session | Scoped session callback token. |
| `* /ai/trickle/{channel...}` | Generic channel traffic | Use the returned channel URL; see the [Trickle protocol](../trickle/README.md). |

SDK revisions, fixture setup, and compatibility test coverage are documented in
[development](development.md#sdk-integration-fixtures).

`RunnerServiceURL` can advertise a separate runner-facing base URL for the
heartbeat `orchestrator` and orchestrator-to-runner (O2R) control channel,
session callback header, and trickle
`internal_url`. Public discovery, session, proxy and channel URLs use
`ServiceURL`. The default generated proxy path is `/run/{proxy}`;
`/proxy/{proxy}` remains an accepted alias. O2R channels send periodic
`{"keep":"alive"}` messages while the runner is registered.

## Remote signer HTTP

| Method and route | Response / request |
| --- | --- |
| `POST /sign-orchestrator-info` | JSON `address` and `signature`. |
| `POST /generate-live-payment` | JSON request with Protobuf `orchestrator` bytes and signed client state; supports `live` and `fixed`. |
| `GET /discover-orchestrators` | Discovery JSON, including runner information. |

`POST /sign-orchestrator-info` requires no request body. It returns the signer's
Ethereum account address and a hex signature over that address. The discovery
response's `address` is an orchestrator service URL, not an Ethereum address.

### Payment requests and responses

`POST /generate-live-payment` accepts a JSON body:

| Field | Format and behavior |
| --- | --- |
| `orchestrator` | Required Base64-encoded `OrchestratorInfo` Protobuf; use the challenge's `payment_params`. |
| `type` | Required `live` or `fixed`. |
| `ManifestID` | Defaults to the challenge's auth session ID; if supplied, it must match. |
| `app` | Optional application name, bound into subsequent signed state. |
| `state` | Omit on the first call; thereafter, pass the returned object unchanged. Its `state` and `sig` fields are Base64 strings. |
| `maxPrice` | Optional positive wei ceiling: `price`, `currency: "wei"`, and `unit: "seconds"` for `live` or `"fixed"` for `fixed`. |

Success returns `payment`, `segCreds`, and `state`. Send the first two as the
orchestrator's `Livepeer-Payment` and `Livepeer-Segment` headers. State is bound
to the ticket recipient, application, payment type, and manifest ID.
Earlier state can be replayed; HTTP retries can generate separate tickets.

Requests are limited to 1 MiB, with a five-second body deadline, a 30-second
request deadline, and 64 concurrent requests. Oversized payment request bodies
return 400; admission exhaustion returns 503.

### Discovery filters and results

The signer appends `/discovery` to each base URL in `Orchestrators` and queries
those sources on every request, with a five-second timeout and 1 MiB response
limit per source. It filters runners by exact `app` and `gpu` query values;
`gpu` matches the GPU name. Repeated values match any value within that filter;
when both filters are present, a runner must satisfy both. `caps` does not
filter this endpoint.

Each result contains `address`, `score` (currently 1), `runners`, and a UTC
`last_seen` timestamp for the successful response. Runner records carry their
URL, application, mode, capacity, optional metadata/GPU, and advertised prices.
Discovery does not apply the signer's payment price ceilings or merge duplicate
entries. Failed or blocked sources are omitted when another source
succeeds. A successful source with no matching runners yields HTTP 200 and
`[]`; no configured sources or failure of every source yields 503.

Grants apply to outbound connections; returned URLs are not filtered. See
[discovery configuration](signer.md#configure-discovery) for setup.

### Payment limits and refresh

Configured USD ceilings are `MaxHourlyPrice / 3600` for live work and
`MaxFixedPrice` for fixed work. Request and webhook wei ceilings also apply;
the price cannot exceed the initial session price. Comparisons are exact.

`ETHUSDFeed` supplies the conversion rate unless `WeiPerUSD` overrides it.
Feed observations refresh every hour and expire after `ETHUSDMaxAge`
(default two hours). A missing or stale rate makes payment generation and
readiness return 503; `/sign-orchestrator-info` and discovery remain available.
Fixed rates never refresh or expire.

| Ticket limit | Default and scope |
| --- | --- |
| `MaxTicketEV` | 3,000,000,000,000 wei expected value per ticket. |
| `MaxBatchEV` | 20,000,000,000,000 wei expected value per batch. |
| `DepositMultiplier` | 1; face value cannot exceed deposit divided by this value. Must be at least 1. |
| Batch size | At most 100 tickets; issued credit covers at least the greater of the fee and one ticket's EV. Unused credit remains in signed state. |

Payment generation requires payer deposit and recipient-claimable reserve,
complete creation-round metadata, and a nonzero, 20-byte ticket recipient.
The supplied creation round and hash are retained.
Parameters within one L1 block of expiry, authentication within three minutes
of expiry, or a carried ticket nonce at least 500 require refresh (480).
A new recipient randomness hash resets the nonce; individual
ticket nonces must be from 1 through 599. On Arbitrum, parameter expiry uses the L1
clock. Parameter expiry is supplied by the caller; these checks do not establish
the provenance of an arbitrary challenge.

### Authorization webhook

To approve or reject payments, configure an authorization webhook. The signer
sends it a `POST` with `Content-Type: application/json` before signing. The body
contains `headers` (the client's request headers, with an array of values per
name) and `state` (the proposed payment state). Use these to identify the caller
and evaluate the payment against your policy. The ticket batch is not included.
Example request, showing selected state fields:

```json
{
  "headers": {"Signer-Auth-Id": ["auth-456"]},
  "state": {
    "StateID": "session-1",
    "ManifestID": "manifest-1",
    "App": "example",
    "Type": "live",
    "SenderNonce": 7,
    "Balance": "500/1",
    "SequenceNumber": 3
  }
}
```

The webhook must return HTTP 200 with a JSON decision:

| Field | Required | Meaning |
| --- | --- | --- |
| `status` | Yes | 200 approves; a value from 400 through 599 rejects with that signer status. |
| `reason` | No | Optional string; never returned to the client. The signer generates rejection messages. |
| `expiry` | No | Unix seconds until approval may be reused. Omitted, zero, negative, or expired values authorize each call. |
| `auth_id` | No | Authorization identity; takes precedence over the incoming `Signer-Auth-Id`. |
| `maxPrice` | No | Positive wei ceiling in the request's payment unit; retained during cached approval. |

Example approval with an optional price ceiling:

```json
{"status": 200, "auth_id": "auth-456", "maxPrice": {"price": 1300, "currency": "wei", "unit": "seconds"}}
```

For rejection, still return HTTP 200, with the desired error in `status`:

```json
{"status": 403}
```

An unavailable webhook, redirect, non-200 HTTP response, invalid decision,
or malformed webhook price ceiling returns 502. Invalid request price ceilings
return 400; exceeded ceilings return 481.
Changing an established auth ID returns 403. Changing the configured webhook
URL or headers invalidates cached approval. The final auth ID must match
established state; omitting `Signer-Auth-Id` does not authenticate a caller. During
cached approval, signed state is a bearer credential.

Webhook header files accept one comma-separated record of `Header: value`
entries. Names are case-insensitive; repeated names retain multiple values.
Quote an entire entry using CSV syntax when its value contains commas, for
example `"X-List: a,b",Authorization: Bearer credential`.

`pm/wire` retains only the consumed fields and original field numbers:
`OrchestratorInfo` 1–4 and 6, `TicketParams` 1–7, `PriceInfo` 1–2,
`AuthToken` 1–3, `TicketExpirationParams` 1–2, `Payment` 1–5, payer params
1–2, and `SegData` 1, 3, 5 and 8. Other fields are skipped on decode.
Binary fixtures produced by the pinned Python runner check the wire
format in `pm/wire/wire_test.go`. Capability payloads and `lv2v` requests
are rejected.

## Lifecycle and response codes

Registration defaults omitted capacity to one and normalizes price unit and
currency whitespace and case. See [sessions and proxies](orchestrator.md#sessions-and-proxies)
for session pricing and release behavior, and the [Trickle protocol](../trickle/README.md)
for channel behavior.

| Signer status | Client action |
| --- | --- |
| 400 | Correct request fields, ticket exposure, price ceilings, signed state, or payer funds/chain access detected during precheck. |
| 403 | Correct an authorization identity mismatch or review the webhook's rejection policy. |
| 413 | Reduce the accounting event size; no payment credentials were returned. |
| 480 | Refresh payment parameters or orchestrator authentication. |
| 481 | The price exceeds a ceiling or the initial session price. |
| 482 | No tickets are needed; skip this payment cycle. |
| 500 | Payment generation or signing failed, including payer funds/chain failures detected after precheck. |
| 502 | Restore the authorization webhook or correct its response. |
| 503 | Retry after payment dependencies, discovery sources, or accounting storage recover. |

Healthy empty discovery returns HTTP 200 with `[]`. Rate and readiness policy
are described in [the signer guide](signer.md). Signed state preserves its
sequence through parameter refresh and resets the ticket nonce for new
randomness. Replicas with the same key and webhook configuration can continue
that state, but there is no server-side replay database: earlier state can be
reused and retries can receive different tickets. Recipients reject duplicates.

Signed state acts as a bearer credential during cached authorization. The
webhook receives the proposed payment state; payment data is returned only
after approval. See [authorization](signer.md#authorization) for identity and
cache behavior.

## Accounting events

Optional Kafka accounting emits `create_signed_ticket` JSON events with
live/fixed fields. Compatibility fields include an empty `gateway`
and zero `pixels`. Events describe the generated payment and final authorization
identity, using the USD rate captured for price validation.

Events are persisted before payment data is returned. Unavailable or full
outbox storage returns 503. Delivery is at least once: retries reuse the same
ID and payload, and consumers must deduplicate IDs. Separate signing requests
emit separate events, including requests whose responses are interrupted after
persistence. See [Kafka accounting](signer.md#kafka-accounting-and-budgets).

The outbox defaults to `events.sqlite` and 256 MiB of pending JSON
payloads; SQLite and WAL overhead are additional. Serialized events exceeding
either the total outbox quota or 1 MiB minus 1 KiB for Kafka framing return 413,
without payment credentials or changing readiness. Failed local persistence
or exhausted capacity returns 503. Kafka outages allow signing while the outbox
has capacity. See [recovery](payment-recovery.md#signer-accounting-recovery)
for backups and replay after restart.

The metrics listener (default `127.0.0.1:8938`) exposes:

- `livepeer_signer_up`
- `livepeer_signer_kafka_pending_events`
- `livepeer_signer_kafka_pending_bytes`
- `livepeer_signer_kafka_oldest_event_age_seconds`
- `livepeer_signer_kafka_outbox_healthy`
- `livepeer_signer_kafka_publish_errors_total`
- `livepeer_signer_kafka_enqueue_errors_total`
- `livepeer_signer_kafka_storage_errors_total`

`/healthz` reports process liveness. `/readyz` requires chain/funds access, a
positive deposit and total reserve, no withdrawal scheduled at or before the
next round, a fresh rate, and healthy enabled outbox storage with capacity.
Kafka connectivity alone does not make readiness fail. Recipient-specific
claimable reserve is checked during payment generation.

Kafka settings use `[Kafka]` in TOML, `--kafka-*` flags, and
`LIVEPEER_SIGNER_KAFKA_*` environment variables. Supplying any setting enables
accounting and requires `Broker` and `Topic`. A broker accepts one `host[:port]`
(bracket IPv6), with TLS/system trust by default; `kafkas://` selects TLS and
`kafka://` selects plaintext. Credentials, paths, queries, and fragments are
rejected. Advertised cluster brokers must support the same transport and auth.

Paired username/password secrets enable SASL; use files or
`LIVEPEER_SIGNER_KAFKA_USERNAME` and `LIVEPEER_SIGNER_KAFKA_PASSWORD`.
`AuthMethod` accepts `plain`, `scram-sha-256`, or `scram-sha-512` (default).
Without credentials, SASL is disabled. An explicit port overrides these signer
conventions:

| Connection | Default port |
| --- | --- |
| Plaintext without SASL | 9092 |
| TLS without SASL | 9093 |
| SASL/SCRAM, with or without TLS | 9096 |
| SASL/PLAIN, with or without TLS | 9094 |

## Codec checks

The selected-field Protobuf decoder is compared with the generated runtime for
merged message fields, unknown and wrong-wire fields, UTF-8, and bounds.
Ticket nonces outside uint32 are rejected. See [development](development.md)
for fixtures and test commands.
