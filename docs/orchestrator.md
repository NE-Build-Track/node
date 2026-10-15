# Running an orchestrator

The orchestrator registers HTTP runners, advertises them through discovery,
reserves sessions, and forwards application traffic. It can run in on-chain or
off-chain mode, supporting persistent sessions and single-shot requests, HTTP
streaming, server-sent events (SSE), WebSockets, callbacks, and
[Trickle channels](../trickle/README.md).

Start with the [local example](../README.md#run-a-local-example). For deployment,
copy the [configuration example](../configs/orchestrator/config.example.toml),
set its paths and destinations, and run:

```sh
bin/livepeer orchestrator --config /etc/livepeer/orchestrator.toml
```

## Register runners

Configure a bootstrap credential, a static runner file, or both.

Dynamic runners register through `POST /runners/heartbeat` using the bootstrap
credential. The response supplies a credential scoped to that runner for later
heartbeats and unregister requests. Set `BootstrapSecretFile` as described in
[configuration and secrets](configuration.md#credential-files). If you use only
dynamic runners, omit `RunnerConfig`.

Static runners are loaded from `RunnerConfig` using `[[Runners]]` tables with
Go field names, including `RunnerURL`, `HealthURL`, and `HealthCode`. Optional
GPU and price tables use `[Runners.GPU]` and `[Runners.PriceInfo]`. Use the
[runner example](../configs/orchestrator/runners.example.toml) to set each
runner's ID, URL, application, mode, capacity, and health check. If you use only
static runners, omit `BootstrapSecretFile`. Their health URLs are checked
every `HeartbeatInterval` (five seconds by default). A runner with a health URL
is unavailable until its first successful check; failed checks release its
sessions.

Discovery at `GET /discovery` lists available runners. Off-chain operation
accepts quoted runner prices but advertises free service. Enable the payment
settings below to charge clients.

## Allow local and private destinations

Runner forwarding, callback-created session proxies, and static health checks
have separate [destination grants](configuration.md#destination-grants). Use
the corresponding setting to allow connections to local or private addresses:

| Setting | Permits private destinations for |
| --- | --- |
| `RunnerGrants` | Application requests to runners |
| `SessionProxyGrants` | Targets supplied when creating a session proxy |
| `HealthGrants` | Static runner health checks |

For a runner and health endpoint at `127.0.0.1:9000`, set both:

```toml
RunnerGrants = ["127.0.0.1"]
HealthGrants = ["127.0.0.1:9000"]
```

To allow a private subnet, use a CIDR such as `"192.168.1.0/24"` in each setting.

Outbound HTTPS uses system certificate trust. Ethereum RPC connections do not
require grants.

## Public URLs and TLS

`ServiceURL` is the public base URL advertised to clients, defaulting to
`http://<Listen>`. Set `RunnerServiceURL` if runners reach the listener through
a different URL such as Docker. That URL is used for heartbeat responses,
session callbacks, Trickle `internal_url`, and the orchestrator-to-runner (O2R)
control channel. Client discovery, session, proxy, and channel URLs continue
to use `ServiceURL`.

The application listener serves HTTP and defaults to loopback. Use explicit
IP:port addresses for `Listen` and `MetricsListen`, including IPv6 addresses
such as `[::1]:8935`. `Listen = "0.0.0.0:8935"` or `"[::]:8935"` binds all
interfaces. Terminate public HTTPS in a reverse proxy and set `ServiceURL` to
its HTTPS address. The separate metrics listener must remain on loopback; see
[probe addresses](configuration.md#local-storage-and-probes).

## Sessions and proxies

Each session keeps the price agreed when it was reserved. Later runner
heartbeats or exchange-rate updates affect new sessions. Stopping a session,
expiry, failed health, unregistering a runner, exhausted credit, or shutdown
cancels active HTTP, SSE, and WebSocket requests and removes session channels
and proxies. Single-shot requests use the same lifecycle. A runner stopping
its own active request should expect that request to end abruptly.

Enabling a runner's proxy creates an automatic proxy. Without a URL
template, generated URLs use `ServiceURL/run/{proxy}`. The earlier
`/proxy/{proxy}` path remains an alias for active proxies. A runner can also
create a session proxy through its callback route with a `target_url`.

`ProxyURLTemplate` accepts a hostname label or final path segment, for example
`https://{proxy}.apps.example` or `https://apps.example/proxy/{proxy}`. Configure
DNS, TLS, and forwarding to this listener for those URLs. Hostname routing uses
the actual Host header. Single-shot domain proxies require a runner ID that is
a lowercase DNS label of at most 63 characters.

## Enable payments

For paid operation, select an on-chain `Network` and supply:

- A redemption account and its password; see
  [Ethereum keystores](configuration.md#ethereum-keystores).
- `RPCURLFile` or `LIVEPEER_ORCHESTRATOR_RPC_URL`: RPC credential.
- `ChainID` and `Controller` for custom networks.
- `TicketFaceValue` and `TicketWinProb`: positive uint256 ticket parameters.
  The winning probability must be less than `2^256 - 1`.
- Exactly one conversion source: `WeiPerUSD` or `ETHUSDFeed` (Arbitrum supplies a feed).

The payment database has a [default path](configuration.md); `RedeemerDB` /
`--redeemer-db` overrides it.

Set `OrchestratorAddress` / `--orchestrator-address` to the registered public
address to keep the orchestrator key offline. The keystore account pays gas;
tickets credit the orchestrator.

Incomplete paid configuration fails startup. The orchestrator address must be active
on the configured chain. Runner prices use USD per hour for live work or USD per
fixed request. Hourly discovery prices are normalized to USD and wei per second.

`WeiPerUSD` sets a fixed conversion. Alternatively, set `ETHUSDFeed` to a
Chainlink-compatible ETH/USD feed on the payment chain. The feed is checked
every hour; `PriceMaxAge` must be positive and defaults to two hours. Startup
requires a valid observation. Stale rates suspend new sessions; existing
sessions retain their initial price.

`RedeemerMaxFeePerGas` optionally caps redemption fees in wei per gas. The
cap is per gas, not a total spending budget. Winning tickets wait until payment
parameters expire before redemption exposes their randomness on-chain. Only a
successful, canonical, finalized receipt settles their liability.

Challenges, balances, and replay guards live in memory; restart requires new
paid sessions. Winning tickets and transaction identities survive in SQLite.
See [payment recovery](payment-recovery.md) for backup and reconciliation.

## Resource limits

The registry accepts up to 256 runners and 10,000 sessions, with 25 generated
proxies per session. Control requests have 64 admission slots and a five-second
body deadline. Streaming requests have 256 separate slots.

The session channel API checks a 1,024-entry channel quota with 256 entries
reserved for runner controls. This is not a global Trickle memory limit.
Trickle retains five segments per channel and limits each segment to 10 MB
(10,000,000 bytes). There is no global byte cap or rolling window that allows a
segment to exceed that limit. Size memory for the channels and segments in use.
See the [protocol reference](../trickle/README.md) for missing-segment and
streaming behavior.
