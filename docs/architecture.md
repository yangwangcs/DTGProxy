# Clean-break architecture

The dependency graph is intentionally one-way:

```text
kernel <- language IR <- language
kernel <- storage contract <- provider implementations
language + storage <- execution capabilities <- execution facade <- process assemblies
```

`dtg-kernel` contains identifiers, bounded values, errors, and deterministic primitives only.
`dtg-language` parses T-Cypher, performs semantic analysis, and emits normalized logical IR. It has
no physical planning, RPC, transaction, or storage-provider behavior.

The execution packages own distributed planning, bounded columnar query execution, temporal
snapshot isolation, Shard/Raft state machines, authenticated follower reads, logical replica
snapshots, Snapshot CSR and built-in algorithms, durable asynchronous analytics, the authoritative
catalog, reconciliation, and generational migration. `dtg-execution` exposes stable composition
facades so processes do not import execution siblings directly.

The storage contract is logical and provider-neutral. Pushdown is permitted only when an immutable
capability manifest proves the exact semantic guarantee; residual evaluation remains authoritative.

The four process crates contain configuration, transport adaptation, lifecycle, telemetry, native
provider construction where required, and dependency injection. Business algorithms remain in the
logical layers.
