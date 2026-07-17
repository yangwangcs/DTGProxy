# Durable Raft WAL and Replica Restart

Phase 2 keeps consensus durability and temporal graph data in separate RocksDB databases. The
Raft WAL owns HardState, ConfState, log entries, and the latest Raft snapshot. The Adapter owns all
Current/History/adjacency/transaction records plus the Replica's term, applied index, epoch, and
safe-time watermarks in one atomic business batch. They are joined by the applied log index, not by
a cross-database transaction.

## Ready ordering

`DurableRaftReplica` follows raft-rs' Ready contract:

1. synchronously persist the snapshot, unstable entries, and HardState in one WAL WriteBatch;
2. release messages whose transmission depends on that persistence;
3. apply committed entries to the Adapter, where business data and `applied_index` advance
   atomically;
4. advance Ready, synchronously persist a LightReady commit-index change, and apply any newly
   committed entries;
5. call `advance_apply` only after Adapter apply succeeds.

A successful client acknowledgement still requires the group-level condition: majority commit
and leader Adapter apply. WAL persistence alone is never a successful business response.

## Restart rule

Opening a Replica reconstructs MemStorage from the durable WAL, opens the Adapter state machine,
and sets `raft::Config.applied` to the Adapter's persisted applied index. Consequently, if the
process dies after the committed log is durable but before Adapter apply, RawNode exposes the
missing committed suffix again after restart. Adapter idempotency and command fingerprints guard
against duplicate or divergent replay.

Startup fails closed when the WAL commit index is behind the Adapter applied index. It also refuses
to start if compaction has moved the WAL first index beyond the Adapter frontier; that state needs
an Adapter checkpoint installation rather than blind log replay.

## WAL record format

Each RocksDB value has a four-byte record magic, format version, bounded payload length, protobuf
payload, and CRC. Entry keys use big-endian indices so iteration is ordered. Persisting a
conflicting suffix deletes the old suffix and writes the replacement atomically. Writes use a
synchronous RocksDB WriteBatch. Compaction is rejected unless a durable Raft snapshot covers the
requested boundary.

The in-memory MemStorage inside `RocksRaftStorage` is a read cache only. A failpoint after the DB
write but before cache update proves that reopening uses the durable database as the source of
truth.

## Current boundary

The WAL can persist and recover a Raft snapshot payload, while `replica-snapshot` can create and
verify an Adapter checkpoint. Installing that checkpoint into a running/lagging Replica and
atomically activating the matching Raft snapshot is the next integration step. Until then,
`DurableRaftReplica` rejects an incoming non-empty Ready snapshot rather than exposing a Replica
whose consensus position and temporal data disagree.
