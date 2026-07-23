# DTGProxy Replica Snapshot

`replica-snapshot` binds a local RocksDB checkpoint to the exact replicated state that produced it.
The current manifest contains shard ID, placement epoch, Raft term/index, closed/resolved/Adapter-applied
timestamps, canonical voter IDs, and a 32-byte checkpoint digest.

The manifest uses explicit big-endian fields, `DTSM` magic, its current format version, bounded membership, and a
CRC-32 checksum. Voter IDs must be nonzero, sorted, and unique. The checkpoint digest is BLAKE3 over
the sorted relative path, length, and complete bytes of every regular file, with domain separation.
Symlinks and other special entries are rejected.

Checkpoint creation first confirms that `ReplicaMetadata.applied_index` equals the Adapter's atomic
`applied_log_index`, then asks RocksDB to create its checkpoint and hashes it. The checkpoint and a
synced manifest are built in a hidden sibling directory. A same-filesystem rename publishes the
complete bundle; failures before that point remove the staging directory and never expose the final
name.

Source log compaction is ordered after bundle publication. `RocksRaftStorage` persists the matching
Raft snapshot synchronously before deleting covered entries and retains every log entry above the
snapshot index. Activation is idempotent, so a crash after the WAL write can retry from the already
published bundle. A crash after bundle publication but before WAL activation leaves the old log
intact.

Follower installation is generation based and never overwrites an open or lagging RocksDB
directory. It copies the checkpoint into a hidden new generation, verifies the digest, opens the
copy, compares every Replica metadata field with the manifest, and seeds a separate Raft WAL with
the same snapshot. Only then is the generation renamed to its visible name. The caller switches
Replica ownership to the returned Adapter/WAL paths; the previous generation can be retained for
rollback and garbage-collected later.

Raft `Snapshot.data` contains the encoded manifest. `install_received_snapshot_bundle` requires
the incoming Raft snapshot's term, index, membership, and payload to exactly equal the supplied
bundle before installation. Because the RocksDB checkpoint is transferred out of band, a
`DurableRaftReplica` deliberately rejects a snapshot that reaches `Ready` without this bundle; the
node supervisor must intercept that snapshot, install a generation, and reopen the Replica instead
of applying consensus position without temporal data.

The recovery tests install a snapshot at index N, append and commit N+1 onward, then reconstruct
RawNode at the Adapter's persisted N. They compare Current, transaction-time AS OF, DIFF, incoming
and outgoing adjacency, all Replica watermarks, and every logical keyspace including entry and
transaction fingerprints. Crash schedules cover checkpoint creation, manifest publication,
Adapter copy, follower Raft snapshot persistence, source WAL compaction, and post-publication
recovery.
