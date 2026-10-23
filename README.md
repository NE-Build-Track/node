# Livepeer Node

Livepeer Node contains services and command-line tools for the Livepeer network.
It includes an orchestrator for Livepeer runners, standalone discovery,
a remote signer for network payments and discovery filtering, and a protocol CLI
for managing wallets, staking, earnings, funding, governance, and local signing.

| Executable | Role |
| --- | --- |
| `livepeer` | Runs the bundled component commands. |
| `livepeer-orchestrator` | Registers runners, reserves sessions, proxies traffic, and redeems payment tickets. |
| `livepeer-discovery` | Collects and caches orchestrator runner inventories without running an orchestrator. |
| `livepeer-signer` | Signs identities and payments, discovers orchestrators, and optionally publishes accounting events. |
| `livepeer-chain` | Inspects accounts and contracts, manages wallets, staking, earnings, funds, and governance, and signs files locally. |

The orchestrator supports persistent and single-shot sessions, HTTP streaming,
server-sent events (SSE), WebSockets, and Trickle channels. It can run locally
without payments or use Ethereum payment tickets. The signer requires chain
configuration.

## Build

Install the Go version declared in [go.mod](go.mod) or newer, then run from the repository root:

```sh
make build
bin/livepeer version
bin/livepeer orchestrator --help
```

## Run a local example

This example forwards a request to a small HTTP service. It runs off-chain.

In one terminal, start the example runner:

```bash
while true; do
  printf 'HTTP/1.1 200 OK\r\nContent-Length: 24\r\nConnection: close\r\n\r\nhello from local runner\n' | \
  nc -l 127.0.0.1 9000 >/dev/null || break
done
```

In a second terminal, from the repository root, start the orchestrator:

```sh
bin/livepeer orchestrator --config configs/orchestrator/local.example.toml
```

In a third terminal, check readiness, discovery, and forwarding. Allow up to
five seconds for the first runner health check before checking discovery:

```sh
curl -fsS http://127.0.0.1:8936/readyz
curl -fsS http://127.0.0.1:8935/discovery
curl -fsS http://127.0.0.1:8935/apps/local-runner/app/hello
```

Expect `ready`, a discovery entry for `local-runner`, and
`hello from local runner`. Readiness uses the separate metrics port. Stop the
runner and orchestrator with Ctrl-C in their terminals.

## Configure a deployment

Use [configuration and secrets](docs/configuration.md) for credentials, encrypted
Ethereum keystores and password files, and probe addresses. Then follow the
component guides:

- [Orchestrator](docs/orchestrator.md): dynamic/static runners, networking, TLS,
  proxy URLs, limits, and payments.
- [Discovery](docs/discovery.md): standalone inventory collection, caching, and peer advertising.
- [Signer](docs/signer.md): chain access, price limits, authorization, and Kafka
  accounting.
- [Chain commands](docs/chain.md): inspecting accounts, previewing and submitting
  transactions, and signing files locally.
- [Payment recovery](docs/payment-recovery.md): backups, restart, and uncertain
  transaction recovery.

Chain state changes perform a dry run by default. Add `--submit` to broadcast
and wait for successful inclusion. Use `--no-wait` for a single transaction
without replacements. Trickle retains five segments per channel, with a 10 MB
limit per segment and no global byte cap.

For tests, SDK fixtures, and packaging, see [development](docs/development.md).
The [documentation index](docs/README.md) links the operating guides and protocol
references.

Imported or adapted code retains its original MIT notice in
[MIT notice](LICENSE).
