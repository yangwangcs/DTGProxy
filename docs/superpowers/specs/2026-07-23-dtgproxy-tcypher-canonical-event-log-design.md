# DTGProxy T-Cypher Canonical Event Log Design

Date: 2026-07-23

Status: approved by delegated technical decision

This document supplements `2026-07-23-dtgproxy-cedar-tcypher-clean-break-design.md`.

## Decision

`CHANGES` reads an immutable canonical event log. It does not reconstruct events from current
projections or use the compacted history chain as an event source.

The current history chain is authoritative for state reconstruction at a system-time snapshot.
Anchors intentionally compact prior deltas and store reconstructed projections, so they do not
preserve an unambiguous PUT/DELETE event stream, per-operation provenance, or a bounded global
event scan contract.

## Event Contract

Each committed temporal mutation emits one `CanonicalTemporalEvent` in the same atomic storage
batch as its identity, current projection, history entry, and adjacency updates. An event contains:

- graph, partition, element kind, and element identity;
- operation (`PUT` or `DELETE`);
- valid interval and `valid_from`;
- commit HLC and a stable per-commit ordinal;
- the mutation payload for PUT and no payload for DELETE;
- vertex label or edge type and endpoints when applicable.

Each immutable event value is referenced by two canonical index keys: a commit-time key ordered by
graph, commit HLC, ordinal, and element, and a valid-time key ordered by graph, `valid_from`, commit
HLC, ordinal, and element. This lets both bounded axes apply row/byte budgets after an index seek,
without teaching an Adapter temporal semantics. Event values are self-contained and checksummed.
No event is rewritten or removed by history compaction. Retention is an explicit future policy,
not an implementation side effect.

## Read Semantics

For `CHANGES FOR VALID_TIME BETWEEN [from,to)`, the engine scans events whose `valid_from` is in
the window, applies the statement's system-time cutoff, and returns immutable PUT/DELETE events.

For `CHANGES FOR SYSTEM_TIME BETWEEN [from,to)`, the engine scans events whose commit HLC is in
the window. Both paths preserve commit sequence and operation provenance. They never rebuild
state and never silently fall back to point or interval state scans.

## Boundaries

The event log is part of DTGProxy's canonical temporal model. Storage adapters persist it through
the existing typed key/value SPI and may expose bounded batch candidate scans, but may not
interpret T-Cypher, choose snapshots, or redefine event semantics. DTGProxy's columnar execution
layer applies residual filtering and projection after retrieval.

## Verification

Tests must prove atomic write visibility, PUT/DELETE preservation, valid- and system-axis window
selection, system snapshot fencing, commit ordering, replay idempotency, bounded scans, and equal
results across memory, RocksDB, PostgreSQL, and Neo4j mappings.
