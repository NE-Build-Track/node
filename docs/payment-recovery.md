# Payment state and transaction recovery

SQLite preserves winning tickets and redemption transactions for
[paid orchestrators](orchestrator.md#enable-payments), and accounting events for
[Kafka-enabled signers](signer.md#kafka-accounting-and-budgets). Orchestrator
sessions, payment parameters, balances, and replay guards are process-local;
restarts rotate temporary keys and start fresh sessions. Stored winners remain
redeemable. Also start fresh sessions after restoring recipient state or
migrating legacy signed state.

## Back up SQLite

Run as the service owner. Use a persistent, owner-only directory (`0700`) and
keep databases and WAL/SHM sidecars owner-only. The orchestrator database contains
ticket signatures and redemption randomness; signer sidecars can contain
accounting payloads. Signer startup rejects permissive or nonregular files.

With the `sqlite3` CLI, back up either database while it is running. Set `db` to
the actual `RedeemerDB` or `Kafka.OutboxDB` path:

```sh
(
  set -eu
  umask 077
  db="$HOME/.lpData/arbitrum-one-mainnet/orchestrator/payments.sqlite"
  test -f "$db"
  backup_dir=$(mktemp -d "$HOME/livepeer-backup.XXXXXX")
  sqlite3 "$db" ".timeout 5000" ".backup '$backup_dir/state.sqlite'"
  test "$(sqlite3 "$backup_dir/state.sqlite" 'PRAGMA integrity_check;')" = ok
  printf 'Backup: %s\n' "$backup_dir/state.sqlite"
)
```

The backup API includes committed WAL contents; no sidecar copies are needed.
For a file copy instead, stop the service and copy the database plus any WAL/SHM
files together. Preserve ownership and permissions on restore. Back up the
clearinghouse's accounting state independently of each signer's outbox.

## Upgrades and rollback

Keep the previous executable and config. [Install](../README.md#install) the new
bundle in a separate directory, stop the affected service through its supervisor
(Ctrl-C for foreground examples), wait for exit, and back up its current state.
Startup applies pending migrations; inspect them before starting. This example
uses an unchanged config and a paid orchestrator:

```sh
previous="$HOME/.local/share/livepeer/v1.0.4/bin/livepeer"
candidate="$HOME/.local/share/livepeer/v1.0.5/bin/livepeer"
config=/etc/livepeer/orchestrator.toml
"$candidate" orchestrator migrate --config "$config" status &&
  "$candidate" orchestrator --config "$config"
```

Check [probes](configuration.md#local-storage-and-probes), discovery, and a
representative application/payment request. To roll back, stop the candidate,
then check the previous binary against the **current** database:

```sh
"$previous" orchestrator migrate --config "$config" status &&
  "$previous" orchestrator --config "$config"
```

Use `signer` and its config for an enabled outbox; omit migration checks for
off-chain orchestrators or signers without Kafka. Restore the previous config
if it changed, keeping network/database paths consistent. Check release notes:
migration status verifies history, not all application compatibility. Unknown
versions or changed migration checksums block downgrade. `migrate down` destroys
data, including payment/accounting tables in the initial migrations. If downgrade
is incompatible, retain the latest state for a compatible fix; restoring an old
snapshot can lose events, winners, and transaction identities.

## Signer accounting recovery

Keep `Kafka.OutboxDB` on persistent local storage, one file per replica. It uses
WAL and FULL synchronous commits; new files are `0600`. Monitor pending payload
bytes and actual disk use.

Restart with the same signer key, Kafka broker, topic and outbox path. Pending events
resume automatically with their original IDs and exact payloads. A nonempty outbox
rejects a changed signer address, broker or topic. Drain it before changing those
settings; credential rotation does not change its destination binding. Broker
addresses learned from Kafka metadata can change without changing the bootstrap
configuration. Never delete pending rows or replace the outbox to clear an outage.

Only acknowledged events are removed. A crash before local deletion, or a partial
or uncertain write, replays the batch; consumers must deduplicate event IDs.
The outbox is a delivery queue, not an audit archive. Restoring a backup can replay
old events and omit newer ones, so preserve the latest outbox during rollback.

Kafka outages allow signing to continue until the outbox runs out of capacity. Failed
local persistence returns 503 without payment credentials. Events are saved before the
HTTP response; connection loss after saving an event does not retract its charge.
Separate client retries emit separate events. Payment state can still move between
replicas with the same key and webhook configuration; outbox files must not be shared.

## Orchestrator transaction recovery

The keystore account signs redemptions; tickets retain their original recipient,
even if `OrchestratorAddress` or the keystore account changes. Redemption attempts
use these states:

| State | Meaning and recovery |
| --- | --- |
| Queued (no attempt) | Wait for parameter expiry. Preparation failures leave the ticket eligible for another attempt. |
| `prepared` | Signed bytes, nonce and hash are durable before sending; restart resumes the saved transaction. |
| `broadcast` | Send in progress or interrupted before recording its outcome; restart resumes the saved transaction. |
| `submitted` | RPC acknowledged the hash; monitor inclusion and replace on timeout. |
| `retryable` | Submission, waiting or replacement failed; retry the ticket automatically. Older hashes remain under receipt reconciliation. |
| `mined` | Canonical inclusion observed; wait for finalization without fee replacement. A reorg resumes recovery. |
| `awaiting_finality` | Latest state says the ticket is used; pause submission until finalized state confirms use or a reorg removes it. |
| `confirmed` / `used` | Successful canonical finalized receipt or finalized ticket-use observation settles liability. |
| `reverted` / `invalid` | Finalized transaction failure or an invalid ticket; stop submitting and reconcile unresolved hashes. |
| `expired` | Ticket creation round is outside its validity window; reconcile unresolved hashes. |
| `superseded` | Finalized inclusion consumed the same sender/nonce, or finalized ticket settlement made this transaction unnecessary. |

Only canonical finalized state settles a ticket. RPC failures, missing receipts,
unfinalized inclusions and reorgs retain uncertainty and liability. Each ticket
counts once even when it has several signed transactions. Previously exposed
recipient randomness stays closed to new payments throughout retries.

`RedeemerTxTimeout` (`--redeemer-tx-timeout`) defaults to five minutes per
inclusion attempt. `RedeemerMaxTxReplacements` (`--redeemer-max-tx-replacements`)
defaults to one; zero disables replacements. Replacements retain the nonce and
calldata and raise both fee caps by geth's default 10% threshold, rounding up
with a strict increase.
Geth estimates initial fees using the RPC header's base fee. A missing base fee
leaves the ticket queued with an error. `RedeemerMaxFeePerGas` optionally caps
initial and replacement fees in wei per gas. Finalization waits do not consume
the inclusion timeout.

Restart restores deadlines, replacement budgets and nonce reservations for
acknowledged sends. Rejected submissions do not reserve a nonce. A rotated key can
resend saved bytes and reconcile receipts, but cannot replace the previous account's transactions.

Migration 002 preserves existing transactions. Its rollback refuses to discard
transactions or metadata that the old schema cannot represent. Retain the upgraded
database for a compatible binary when rollback is refused.

Inspect status without a key or RPC connection:

```sh
livepeer orchestrator redemptions
```

Automatic recovery normally handles rebroadcast. To explicitly resend the
current retryable transaction using its saved bytes and fees:

```sh
livepeer orchestrator redemptions \
  --retry-transaction 0xTRANSACTION_HASH --submit \
  --rpc-url-file /run/secrets/livepeer-payment-rpc-url \
  --chain-id 42161
```

Recovery and migration commands default to `arbitrum-one-mainnet`, using
`~/.lpData/arbitrum-one-mainnet/orchestrator/`. For other networks or directories,
pass the same `--config`, or `--network` and `--data-dir`, as the service.
`--redeemer-db` selects a different database. Recovery reads `RPCURLFile` and `ChainID`
for retries; `--rpc-url-file` and `--chain-id` override them. It does not unlock
the account or read unrelated service credentials. RPC endpoints use normal
system TLS trust. Historical hashes remain inspectable; manual retry cannot
reopen a terminal ticket or displace a newer active transaction.

In-memory control state is bounded: payment sessions have a 100,000-entry quota
and 24-hour inactivity retention; ticket replay guards have a 1,000,000-nonce
quota and are pruned at payment parameter expiry. Active session accounting keeps
session activity current. Expiry is monotonic within a recipient lifetime so
a backward chain observation cannot revive parameters after their replay guards
are removed. Auth-token refresh never resets nonce tracking. Winning tickets
and uncertain transactions are retained for audit/reconciliation, so back up
and monitor database disk use independently of these control-state quotas.
