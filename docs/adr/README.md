# ChronicleDB Architecture Decision Records

Major decisions affecting durability, transactions, recovery,
consensus, replication, snapshots, protocols, or correctness are
recorded here as ADRs.

Each ADR contains Status, Context, Decision, Alternatives Considered,
Consequences, Correctness Implications, and Testing and Proof
Obligations. See [`0000-template.md`](0000-template.md) for the
skeleton.

## Index

| ADR | Title |
|---|---|
| [0001](0001-v1-single-shard-static-cluster-scope.md) | V1 single-shard, static-cluster scope |
| [0002](0002-local-storage-architecture.md) | Local storage architecture |
| [0003](0003-wal-raft-log-responsibility-model.md) | WAL / durable-log / Raft-log responsibility model |
| [0004](0004-mvcc-snapshot-isolation.md) | MVCC model and Snapshot Isolation |
| [0005](0005-transaction-commit-and-conflict-model.md) | Transaction commit and conflict model |
| [0006](0006-requestid-idempotency-and-uncertain-outcomes.md) | RequestID idempotency and uncertain outcomes |
| [0007](0007-deterministic-replicated-state-machine-boundary.md) | Deterministic replicated state-machine boundary |
| [0008](0008-raft-architecture-and-persistent-state.md) | Raft architecture and persistent state |
| [0009](0009-transport-clock-randomness-abstraction.md) | Transport/clock/randomness abstraction |
| [0010](0010-read-consistency.md) | Read consistency |
| [0011](0011-snapshot-and-log-compaction-model.md) | Snapshot and log-compaction model |
| [0012](0012-recovery-and-corruption-policy.md) | Recovery and corruption policy |
| [0013](0013-sql-boundary-and-deferred-functionality.md) | SQL boundary and deferred functionality |
