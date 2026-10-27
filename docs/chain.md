# Chain commands

Run `livepeer chain` or `livepeer-chain` to inspect protocol state, manage funds
and stake, register orchestrators, and vote. Set RPC access in a
[chain config](../configs/chain/config.example.toml) or environment variables.
Accounts are inferred from the keystore; use `--account` or
`LIVEPEER_CHAIN_ACCOUNT` to select one. See [configuration](configuration.md).

```sh
livepeer chain --config /etc/livepeer/chain.toml account get
livepeer chain --config /etc/livepeer/chain.toml stake bond ADDRESS --amount 10
```

Transactions dry-run by default. Add `--submit` to sign, broadcast, and wait for
block inclusion. Submissions need a keystore matching the selected account;
reads and dry runs need no password.

Prefix commands below with `livepeer chain`; add `--config FILE` or `--help` as needed.

## Accounts and protocol state

See [keystore setup](configuration.md#prepare-a-keystore) for account creation.

| Command | Purpose |
| --- | --- |
| `account get` | Configured account's ETH/LPT balances and pending nonce |
| `account create` | Generate a new account and save an encrypted keystore offline |
| `status` | Connected chain and block |
| `round get` | Current round, initialization, and lock status |
| `round initialize` | Initialize the current round |
| `protocol get` | Protocol settings, supply, bonded stake, and participation |
| `contracts` | Protocol contract addresses |

## Amounts and commissions

Amounts use human LPT/ETH units with up to 18 decimal places. Add `--base-units`
for integer token units or wei. Use positive decimal amounts, without exponent
notation; TicketBroker funding allows either component to be zero.

`--amount all` uses the wallet LPT balance for bonding, registration, and transfers;
unbonding uses pending stake, and fee withdrawal uses pending fees, both including
pending earnings. A zero balance fails. TicketBroker funding requires numeric amounts.

`--reward-cut` and `--fee-cut` are the orchestrator's commissions: 0–100 percent,
with up to four decimal places. For example, `1.2345` means 1.2345%.
IDs, rounds, and gas limits are integers; gas prices are always integer wei per gas.

## Stake and withdraw LPT

```text
stake get
stake locks [--withdrawable | --locked] [--from-id ID] [--limit N]
stake bond ADDRESS --amount N|all
stake bond ADDRESS --redelegate
stake unbond --amount N|all
stake cancel-unbond [ADDRESS] --lock-id ID
stake withdraw --lock-id ID
```

Use `stake get` to inspect stake, pending earnings, and the current orchestrator.

Bonding deposits wallet LPT, including any required ERC20 approval. Redelegation
moves existing stake; choose one mode. Unbonding creates a withdrawal lock.

Cancel-unbond restores locked LPT to the current orchestrator if still staked;
otherwise `ADDRESS` is required to choose one.

List outstanding locks with `stake locks`, filtering by either `--withdrawable`
or `--locked`. Paginate with `--from-id` and `--limit` (default 100, maximum 1000).
The limit counts scanned IDs, including spent or filtered locks.

## Register and configure an orchestrator

```text
orchestrator get
orchestrator list [--active]
orchestrator register [--amount N|all | --redelegate | --lock-id ID]
                      --reward-cut PERCENT --fee-cut PERCENT [--service-uri URL]
orchestrator set-config [--reward-cut PERCENT] [--fee-cut PERCENT] [--service-uri URL]
orchestrator reward-caller get
orchestrator reward-caller set ADDRESS
orchestrator reward-caller unset
```

`get` shows registration/settings; `list` shows pool members. To `register` the
configured account, choose one staking source unless already self-bonded:
wallet LPT, existing stake, or an unbonding lock. An omitted service URI preserves
the current value; supply one if unset. URIs must be absolute HTTP/HTTPS without
credentials, queries, or fragments.

`set-config` preserves omitted values; unchanged settings need no transaction.
Commission changes require an unlocked round. Calling `reward-caller set` authorizes another account to call
rewards for the orchestrator; `unset` clears that authorization.

## Fund and withdraw TicketBroker ETH

```text
ticketbroker get
ticketbroker fund --amount DEPOSIT --reserve RESERVE
ticketbroker unlock
ticketbroker cancel-unlock
ticketbroker withdraw
```

Funding sends the sum of deposit and reserve, which must be positive. Unlock
starts the withdrawal period; cancel-unlock cancels it. Check `ticketbroker get`
for balances and the withdrawal round, then withdraw once that round is reached.

Orchestrators act as redeemers, submitting winning tickets against the payer's funds. See
[payment recovery](payment-recovery.md#orchestrator-transaction-recovery) for
redemption status and retry procedures.

## Earnings, rewards, transfers, and votes

```text
earnings claim [--end-round ROUND]
earnings withdraw-fees --amount N|all [--recipient ADDRESS]
reward call [--orchestrator ADDRESS]
token transfer ADDRESS --amount N|all
governance poll vote ADDRESS yes|no
governance proposal vote ID against|for|abstain [--reason TEXT]
```

Claims default to the current round, which must be initialized. Unbonding and
fee withdrawal claim earnings automatically. Fee withdrawals default to the
configured account as recipient. Reward calls also default to the configured
account; the target must be active and the caller authorized.

## Sign files locally

```text
sign message --message-file /path/to/message
sign typed-data --data-file /path/to/typed-data.json
```

Signing needs a keystore and password, but no RPC. If an account address is
configured, it must match the signing account. Message signing preserves exact
file bytes, including newlines, and uses the Ethereum message prefix. Typed-data signing accepts EIP-712
JSON with `types`, `primaryType`, `domain`, and `message`. Output includes the
address, hash, and hex signature with recovery ID 27/28.

## Control fees and waiting

```text
gas get [--max-priority-fee-per-gas N]
```

Use `gas get` to check current gas prices and the configured fee ceiling.

| Flag | Effect |
| --- | --- |
| `--gas-limit N` | Override gas units for each transaction; otherwise estimate |
| `--max-priority-fee-per-gas N` | Initial tip; otherwise use the RPC suggestion |
| `--max-fee-per-gas N` | Reject calculated fee caps above this ceiling |
| `--transaction-timeout DURATION` | Wait per attempt; default `3m` |
| `--max-transaction-replacements N` | Allow fee-bumped retries; default `0` |
| `--no-wait` | Broadcast without waiting; single transactions without replacements only |
| `--quiet` | Suppress normal transaction output; errors retain known hashes |

The fee cap is twice the base fee plus the tip. Gas limit × fee cap bounds each
transaction's gas fee, not the total cost of a workflow.

Multi-step workflows show all planned steps, deferring dependent simulations until
prerequisites confirm. If a later step fails, earlier confirmed steps remain
applied: inspect state and repeat the dry run before retrying.

Replacements preserve the action and nonce, raising fee and tip caps by at least
11% using fresh suggestions. An explicit initial tip may rise. Each attempt gets
a full timeout and must respect the ceiling. Inclusion ends waiting; provider
errors and reverts fail.

## Scripting

Use `--output json` for scripts. Amounts are exact decimal strings in base units:
`*_base_units` for LPT and `*_wei` for ETH. Rounds and lock IDs are strings too;
commissions and participation are decimal percentages. Protocol JSON retains
integer ratios: 1,000,000,000 represents 100% for inflation, inflation change,
and target bonding rate; 1,000,000 represents 100% for round lock amount.
Text output converts amounts and ratios to human units.

Transactions emit multiple JSON records for simulations, broadcasts, submissions,
and confirmations. Exit status is 0 on success and 2 on failure.
Action inputs and output/transaction controls are CLI-only, except the fee ceiling
(`MaxFeePerGas`), which also accepts TOML and environment configuration.

## Help and shell completion

```text
help [COMMAND]
completion bash|zsh|fish|powershell
```

Use `completion SHELL --help` for installation instructions for that shell.
