# DTGProxy Replica Snapshot

`replica-snapshot` binds a local RocksDB checkpoint to the exact replicated state that produced it.
The V1 manifest contains shard ID, placement epoch, Raft term/index, closed/resolved/Adapter-applied
timestamps, canonical voter IDs, and a 32-byte checkpoint digest.

The manifest uses explicit big-endian fields, `DTSM` magic, version 1, bounded membership, and a
CRC-32 checksum. Voter IDs must be nonzero, sorted, and unique. The checkpoint digest is BLAKE3 over
the sorted relative path, length, and complete bytes of every regular file, with domain separation.
Symlinks and other special entries are rejected.

Checkpoint creation first confirms that `ReplicaMetadata.applied_index` equals the Adapter's atomic
`applied_log_index`, then asks RocksDB to create its checkpoint, and only then hashes it and returns
the publishable manifest. Restore verifies the complete directory before opening RocksDB and again
checks the Adapter index against the manifest. Tests prove that a modified file is rejected and
that a checkpoint at index N can reopen on an empty Replica, replay entries N+1 onward, and converge
to the source business value and all Replica watermarks.

The manifest is only the Adapter half of a Raft snapshot. Phase 2 Task 5 remains incomplete until a
process-crash durable Raft WAL, snapshot installation protocol, full Current/AS OF/DIFF/adjacency
equivalence checks, crash-point tests, and post-manifest log compaction are implemented.
