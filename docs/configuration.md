# Configuration and secrets

Services read TOML (`.toml`) or JSON (`.json`). Unknown keys are rejected;
migration and recovery commands read needed settings and ignore unrelated keys.
Use the [orchestrator](../configs/orchestrator/config.example.toml),
[signer](../configs/signer/config.example.toml), and
[chain](../configs/chain/config.example.toml) examples as starting points.

Configuration precedence is **CLI > environment > config > default**. Select an
explicit file with `--config FILE` or the component's `CONFIG` environment
variable; the flag wins. An explicit config can set both `Network` and `DataDir`.
`--config=""` disables discovery. Empty environment values are ignored.

`Network` defaults to `arbitrum-one-mainnet` for chain, signer, and the
orchestrator's `redemptions` and `migrate` commands. The orchestrator service
defaults to `offchain`. Arbitrum supplies chain, Controller, and ETH/USD feed
defaults. Custom networks require explicit chain and Controller settings.
Signers and paid orchestrators also need a feed or fixed conversion rate.
Paid orchestrators must select an on-chain network. RPC chain IDs are checked.

By default, component config and storage use `~/.lpData/<network>/<component>`;
accounts use the shared `~/.lpData/<network>/keystore`. Network names must be
nonempty directory names, excluding `.` and `..`.

A supplied `DataDir` is used directly, even when it equals the calculated default.
Discovery looks for `config.toml` there; database defaults are `payments.sqlite`
(orchestrator) and `events.sqlite` (signer). Accounts use `<DataDir>/keystore`.

Explicit configs determine the effective network before default directories are
calculated. Without `--config`, each command loads `config.toml` from its data
directory if present. That file can't set `DataDir`, and its `Network` must match
the network selected by flag, environment or default. For example, a paid
orchestrator using discovery needs `--network arbitrum-one-mainnet`.
Missing discovered configs are skipped; explicit configs must exist, even when
their path matches the discovery path.

Relative config and input paths from CLI/environment use the working directory.
Input paths in config files use that file's directory. Relative database paths
use the effective datadir. Absolute paths stay unchanged. These rules also apply
to account creation, migrations, and recovery commands.

## Names and environment variables

All component TOML keys use Go field names, such as `ServiceURL`, `RPCURLFile`,
and `KeystoreFile`. Command-line flags use kebab case. Environment variables use
the component prefix and underscores:

| Component | Prefix | Example |
| --- | --- | --- |
| Orchestrator | `LIVEPEER_ORCHESTRATOR_` | `LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET` |
| Signer | `LIVEPEER_SIGNER_` | `LIVEPEER_SIGNER_RPC_URL` |
| Chain | `LIVEPEER_CHAIN_` | `LIVEPEER_CHAIN_RPC_URL` |

Run `bin/livepeer COMPONENT --help` for flags and defaults. The orchestrator and
chain support `--print-config` to print configuration with secrets omitted:

```sh
bin/livepeer orchestrator --config /etc/livepeer/orchestrator.toml --print-config
bin/livepeer chain --config /etc/livepeer/chain.toml --print-config
```

Printed configuration also omits secret file paths and the static runner file
path. Chain action inputs, transaction switches, `--output`, and `--print-config`
must be supplied on the command line. Orchestrator `--print-config` and
redemption `--submit` are also command-line-only controls.

## Destination grants

Grants allow local, private, or other restricted destinations; public addresses
need none. Permissions apply per purpose, including redirects.

All `*-grants` accept a literal IP, `IP:port` (`[IPv6]:port` for IPv6), or a CIDR
such as `192.168.1.0/24` or `fc00::/7`. A bare IP or CIDR permits any port.
Grants apply only to literal IP destinations, not hostnames resolving to them.

## Credential files

RPC URLs, bootstrap credentials, keystore passwords, authorization webhook URLs
and headers, and Kafka credentials are secrets. Supply each through its file
option or direct environment variable. Direct secret flags and TOML values are
rejected, and a direct environment secret cannot be set together with its file
option.

Credential files are read as exact bytes, including trailing newlines. For
example, create a bootstrap credential without a newline:

```sh
umask 077
openssl rand -hex 32 | tr -d '\n' > runner-bootstrap.secret
```

Set `BootstrapSecretFile` to that file's absolute path and give the same
credential to runners during initial registration. Secret-manager mounts must
also contain exactly the intended bytes. URL credential files must contain a
valid URL without a trailing newline.

## Ethereum keystores

All three apps discover encrypted geth accounts in the shared network keystore
when `DataDir` is absent, or `<DataDir>/keystore/` when it is supplied.
A sole account is selected automatically. Use `--account` (`Account` in TOML) to
select among accounts, or `--keystore-file` to select a file directly and bypass
discovery.

| App | Keystore TOML key | Password-file TOML key |
| --- | --- | --- |
| All three | `KeystoreFile` | `KeystorePasswordFile` |

Signing requires the selected keystore's password.
Supply the password in a file (`--keystore-password-file`) or an environment
variable. Environment variables use the app prefix, such as
`LIVEPEER_SIGNER_KEYSTORE_PASSWORD_FILE` for a password file or
`LIVEPEER_SIGNER_KEYSTORE_PASSWORD` for the password itself. CLI values override
environment values, which override TOML.

Keystore and password files must be regular files readable only by their owner;
`0400` and `0600` are accepted. Secret-manager symlinks are accepted when their
target files meet these requirements. Password files are read as exact bytes,
including whitespace and trailing newlines. Supply the actual password without
an unintended newline. To use a keystore without a password, supply an empty
password file.

The signer requires a keystore and password at startup. The orchestrator requires
both when payments are configured. The chain requires both for submissions and
local signing. Changing the keystore or password requires a service restart, or
a new chain invocation.

The signer account is the payer, supplying the TicketBroker deposit and reserve.
The orchestrator's keystore account signs redemptions and pays gas. Ticket payments
credit `OrchestratorAddress`, which defaults to that account. The chain account
must match the keystore account when signing. Keep a redemption key exclusive to
its orchestrator worker; concurrent CLI or other use can conflict with nonce
tracking.

### Prepare a keystore

Create an encrypted account locally with the chain CLI. First prepare an
owner-only password file in an existing directory:

```sh
umask 077
openssl rand -hex 32 | tr -d '\n' > /path/to/password
bin/livepeer chain account create \
  --keystore-password-file /path/to/password
```

Creation requires no RPC or account address. It encrypts a freshly generated key
with geth's standard scrypt settings and writes the keystore with `0600`
permissions in the shared network keystore, or `<DataDir>/keystore/` when
`DataDir` is supplied. An optional `--keystore-file` chooses a
new file in an existing parent directory; existing paths are never overwritten. The
command prints the new public address; add `--output json` for scripts. The
password can also come from `LIVEPEER_CHAIN_KEYSTORE_PASSWORD`.

## Local storage and probes

Missing storage directories are created with `0700` permissions; existing
directory permissions are preserved. Keep SQLite files on persistent storage,
and use separate database paths for separate instances.
For signer accounting, use an owner-only parent directory and preserve those
permissions on SQLite's WAL and SHM files during restore.
See [payment recovery](payment-recovery.md) for backup and restart procedures.

Health and metrics endpoints use separate loopback listeners:

| Component | Default application address | Default metrics address |
| --- | --- | --- |
| Orchestrator | `127.0.0.1:8935` | `127.0.0.1:8936` |
| Signer | `127.0.0.1:8937` | `127.0.0.1:8938` |

```sh
curl -fsS http://127.0.0.1:8936/readyz
curl -fsS http://127.0.0.1:8938/readyz
```

Both metrics listeners serve `/healthz`, `/readyz`, and `/metrics`.
Orchestrator readiness reports that the listener is running; it does not check
runner health or payment dependencies. Signer readiness checks its payment
dependencies and, when enabled, local accounting storage. See the component
guides for those checks.
