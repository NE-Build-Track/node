# Livepeer Node

Livepeer Node provides runner orchestration, discovery, payment signing, and
protocol commands for the Livepeer network.

| Executable | Role |
| --- | --- |
| `livepeer` | Runs the bundled component commands. |
| `livepeer-orchestrator` | Registers runners, reserves sessions, proxies traffic, and redeems payment tickets. |
| `livepeer-discovery` | Collects and caches orchestrator runner inventories without running an orchestrator. |
| `livepeer-signer` | Signs identities and payments, discovers orchestrators, and optionally publishes accounting events. |
| `livepeer-chain` | Inspects accounts and contracts, manages wallets, staking, earnings, funds, and governance, and signs files locally. |

## Install

Linux releases include all executables and need no Go installation. This example
uses the GitHub CLI, `tar`, and `sha256sum`; choose a release and architecture:

```sh
set -eu
version=v1.0.5
arch=amd64 # or arm64
release_dir="$HOME/.local/share/livepeer/$version"
(
  mkdir -p "$release_dir"
  cd "$release_dir"
  gh release download "$version" --repo livepeer/node \
    --pattern "livepeer-$version-linux-$arch.tar.gz" --pattern SHA256SUMS
  sha256sum --check --ignore-missing SHA256SUMS
  tar -xzf "livepeer-$version-linux-$arch.tar.gz" --strip-components=1
  sha256sum --check SHA256SUMS
)
export PATH="$release_dir/bin:$PATH"
livepeer version
```

Keep the executables together: `livepeer` dispatches to its sibling components.
Persist the PATH setting in your shell or use absolute executable paths in your
service configuration. For other platforms or source builds, see
[development](docs/development.md).

## Run a local example

This example forwards a request to a small HTTP service. It runs off-chain.

In one terminal, start the example runner:

```bash
while true; do
  printf 'HTTP/1.1 200 OK\r\nContent-Length: 24\r\nConnection: close\r\n\r\nhello from local runner\n' | \
  nc -l 127.0.0.1 9000 >/dev/null || break
done
```

In a second terminal, from the repository root with `livepeer` on PATH, run:

```sh
livepeer orchestrator --config configs/orchestrator/local.example.toml
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

Start with [configuration and secrets](docs/configuration.md), then choose a
component guide from the [documentation index](docs/README.md). Use
[payment recovery](docs/payment-recovery.md) for backups, upgrades, rollback, and
transaction reconciliation; [development](docs/development.md) covers tests,
SDK fixtures, and release packaging.

Imported or adapted code retains its original MIT notice in
[MIT notice](LICENSE).
