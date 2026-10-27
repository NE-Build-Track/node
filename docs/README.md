# Documentation

Start with [installation](../README.md#install) and the
[local example](../README.md#run-a-local-example).

## Operating a node

| Guide | Use it to |
| --- | --- |
| [Configuration and secrets](configuration.md) | Set TOML, environment variables, flags, and credentials. |
| [Orchestrator](orchestrator.md) | Register runners, configure networking and proxies, and enable payments. |
| [Discovery](discovery.md) | Collect and advertise orchestrator inventories without local runners or payments. |
| [Signer](signer.md) | Configure price limits, authorization, discovery, and Kafka accounting. |
| [Chain commands](chain.md) | Inspect accounts, manage stake and funds, and submit transactions. |
| [Payment recovery](payment-recovery.md) | Back up state, upgrade or roll back, and recover accounting/redemption work. |

## Developing and integrating

| Reference | Contents |
| --- | --- |
| [Development](development.md) | Build and test commands, SDK fixtures, and CLI snapshots. |
| [HTTP API](http-api.md) | Routes, payment message fields, and response codes. |
| [Trickle protocol](../trickle/README.md) | Channel operations, sequences, streaming, and limits. |
| [Contract bindings](../eth/contracts/README.md) | Binding provenance and upstream updates. |
