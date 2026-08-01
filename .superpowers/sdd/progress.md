# DTGProxy implementation progress

Three-backend end-to-end diagnostic (2026-07-31):
Task 1: complete (commits 53aaa02..d017df8, review clean)
Task 2: complete (commits d017df8..89ec2fb, review clean)
Task 3: needs fixes (commit 89ec2fb..4cd3613; Critical: process query path skips Filter/Project/Aggregate; Important: ports are not reserved through child startup)
Closure Task 1: complete (commits 4988461..2ffd320, review clean)
Closure Task 2: complete (commits 2ffd320..017c37b, review clean)

Task 1: complete (uncommitted worktree; reviews fixed; shard-runtime 48/48; concurrent MERGE 20/20 twice)
Task 2: complete (uncommitted worktree; multi-row atomic MERGE and explicit Bolt staging; relevant
suite green; independent review clean)
Task 3: complete (uncommitted worktree; six-package suite green; same reviewer Ready with no
Critical or Important findings)
Task 4: complete - current-only Cypher 25 query composition is implemented without a versioned
compatibility namespace.
Task 4A: complete (uncommitted worktree; nine-package suite green; same reviewer Approved with no
Critical, Important, or Minor findings)
Task 4B: complete (uncommitted worktree; 13-package suite 253/253 green; same reviewer Approved
with no new findings)
Task 4C: complete (child-local read/write prefixes, multi-row export multiplication, isolated
Apply/EXISTS/COUNT, auto-commit batch subtransactions, explicit-tx rejection, ordered summaries,
durable distributed replay ledger, and deterministic later-batch partial-failure regression).
Shared-Nothing traversal: point-in-time and interval cross-Shard destination hydration complete.
TemporalJoin: logical/physical/distributed point and interval inner/left semantics complete; focused
coordinator and temporal-row suites green.
Analytics core: canonical Snapshot/Event/Interval/Delta projections, identity-safe parallel-edge
Delta, distributed-native Degree/WCC/PageRank with deterministic PrimaryReplica/Shared-Nothing
equivalence, bounded gathered fallback, and typed paginated job submit/status/results/cancel are
complete. Gateway now projects Interval across all shards before endpoint validation and projects
Delta by comparing two valid-time endpoints at one transaction snapshot. Interval procedures retain
temporal region/provenance. Storage scan usage and bytes are shared across shard and two-view
budgets; exhausted budgets fail before another shard is scanned.
Temporal algorithms added and reviewed: windowed components, windowed triangle count, change-point,
rename-invariant bounded temporal motif count, interval components, and delta summary. Cancellation
is polled inside dense local graph loops and temporal event-window materialization. Gateway tests
cover typed parameterized submit and positive/negative result pagination with stable error codes.
Stable ordinary algorithm catalog completion: DFS, bounded weighted APSP, weighted Brandes
betweenness, weighted closeness, and deterministic hierarchical Louvain are implemented and routed
through the typed Provider. Weighted Brandes rejects non-positive weights and preserves parallel-edge
shortest-path multiplicity; Louvain aggregation excludes supernode self-loops from candidate-community
gain. Analytics API/runtime regression suites and independent re-review are clean.
Remaining main functionality before the unified boundary audit: in-flight Shard/DataNode and
backend process-restart certification, combined Gateway/Meta/Shard/backend restart sequences, the
Meta tombstone protocol for unknown or non-terminal latest-pinned orphans, and final workspace
quality/requirement audit.

Cluster analytics ledger foundation: the current design now uses a Meta-Raft-replicated ledger,
not process-affine job IDs. Canonical cluster job IDs, request-level idempotency, job/topology/lease
revision fencing, deterministic replay retention, monotonic job-ID non-reuse, terminal pruning,
tombstone compaction watermarks, fixed-snapshot/provider-version manifests, dynamic lifecycle
capacity reservation, single-job codecs, bounded claimable listing, and Meta submit/get/list RPCs
are implemented. Meta snapshots preserve the ledger independently of Catalog Revision. Tests cover
single-node restart and three-replica leader change followed by a new owner claiming the same job.
The current Gateway coordinator and Shard Artifact path are authoritative; no legacy process-local
analytics job manager or versioned compatibility path is retained.

Shard analytics artifact storage: complete (uncommitted worktree; current-only Meta namespace,
digest-chained bounded chunks, generation fences and sealed deletion, fail-closed corruption checks,
8-chunk bounded deletion validation, memory/RocksDB recovery coverage; three review rounds ended
Approved with no Critical, Important, or Minor findings). Remote Shard artifact RPCs and Gateway
coordination are implemented and covered by the three-backend deployment matrix.

Durable Shard artifact ReadIndex barrier: complete (uncommitted worktree; real raft-rs ReadIndex
and Ready.read_states lifecycle, current-term commit deferral, leader/term/durable placement-epoch
fencing, bounded inactive cancellation correlations, deadline/drop/shutdown cleanup, stable capacity
mapping, old-leader Artifact Get rejection, single-node and three-replica quorum/recovery coverage;
shard-runtime 64 tests and data-node 53 tests green, strict Clippy clean, third review Approved with
no Critical, Important, or Minor findings).

End-to-end bounded Shard Artifact streaming: complete (uncommitted worktree; Manifest-bound request
validates chunk count, total bytes and full BLAKE3 content digest; shared client validator enforces
ordinal, stable non-zero ReadIndex, digest chain, payload digest and a single explicit terminal
error; Embedded and DataNode streams are poll-driven with at most eight buffered chunks and no
pre-read, release the runtime mutex before consumption and cancel pending reads on drop; generic
Execute/Read/Scan cannot bypass the explicit Artifact RPC or reserved Meta namespace in either
Embedded or Remote mode). Fresh verification: shard-client 12/12, data-node 54/54,
cluster-protocol 6/6, shard-runtime 64/64, four-crate strict Clippy and targeted format/diff checks
green; independent re-review Approved with no Critical, Important or Minor findings. Published
generation pinning and superseded-generation deletion are complete.

Published Artifact generation pin/fencing: complete (uncommitted worktree; current-only explicit
Pin Raft command/RPC and Embedded/Remote ShardClient method; bounded full-generation validation
before one atomic per-generation pin mutation; same-contract idempotency and conflicting-contract
rejection; pinned data cannot be appended, overwritten or deleted even after a newer generation is
created; Get requires an exact intact pin after a real ReadIndex; generic Execute rejects
Put/Pin/Delete while generic Read/Scan reject Meta). Memory, RocksDB, process-restart and
three-replica recovery/fencing coverage is green; five-package serial suite ran 155 tests, targeted
strict Clippy and format/diff checks passed, and independent review Approved with no Critical,
Important or Minor findings. Cluster Coordinator SPI replacement of the process-local job manager
is the next implementation step.

Three-backend closure Gate 1: complete (uncommitted worktree). Gateway analytics submission retry
identity no longer includes newly allocated transaction/Catalog/Topology/Backend/provider fences.
The first durable JobSpec retains every execution fence, while an identical stable submission
intent returns the Meta-canonical original job ID. The original cross-Shard Gateway regression and
the analytics-ledger/procedure-runtime/meta-node/gateway-node serial suites are green. Targeted
strict Clippy is green; the whole-workspace check still reports pre-existing warnings outside this
change.

Three-backend closure Gate 2: complete (uncommitted worktree). `TemporalBackendMapping` is the
current backend extension SPI with versioned schema fingerprints, capability validation,
prepare/apply/commit/abort lifecycle, canonical getter/export/restore operations and an internal
`MappingBackedAdapter` bridge. Registry fences Factory and opened Mapping descriptors. RocksDB is
Mapping-certified and passes the shared TCK for replay, atomic failure, ordered reads, keyspace
isolation, delete/history retention, canonical export/restore equivalence and continued writes.
All three backends now also call the same temporal-graph Mapping TCK covering vertex/edge identity,
current state, vertex/edge history, cross-partition outbound/inbound adjacency, temporal deletion,
byte-identical canonical restore and continued writes.

Three-backend closure Gate 3: PostgreSQL native Mapping implementation complete at backend scope
(uncommitted worktree). The old `canonical_kv` layout has been replaced by native identity,
current, history, outgoing/incoming adjacency, opaque protocol, replay and Replica metadata tables.
The Factory publishes `postgresql-native-temporal/1.0.0` through `MappingBackedAdapter`; prepared
apply has no visibility before commit, and one `SERIALIZABLE` transaction durably commits business
rows, replay state and applied index. Core typed canonical decoding validates graph records and
reconstructs byte-identical keys/values. A disposable PostgreSQL 17 instance passed the non-ignored
package suite, shared Mapping TCK, native vertex/edge/history/cross-partition adjacency tests,
restart/export/restore/continue and RocksDB↔PostgreSQL migration. The full distributed two-mode
matrix remains Gate 5; Gate 4 Neo4j native Mapping is next after Gate 3 review reconciliation.

Three-backend closure Gate 4: complete at backend scope (uncommitted worktree). Neo4j publishes
`neo4j-native-temporal/1.0.0` through `MappingBackedAdapter`, fences instance schema and Mapping
fingerprints, and materializes typed identity/current/history/adjacency nodes, stable endpoint nodes
and native `DTG_EDGE` relationships. One atomic Cypher statement commits business records, exact
mutation replay fingerprints, applied-log fingerprints and the durable applied index. Divergent
mutation replay is checked again inside that statement, closing the prepare-to-commit race; deleting
an EdgeIdentity hides its native relationship without requiring endpoint fields in the delete
mutation. A disposable Neo4j 5.26 Community instance passed 9 unit tests, 5 non-ignored live tests,
the generic Mapping TCK and the shared temporal-graph Mapping TCK. RocksDB and disposable
PostgreSQL 17 pass that same graph TCK. Strict Clippy over temporal-storage and all three adapters is
clean. This is real-backend certification at Mapping scope, not distributed certification: the six
migration directions and PrimaryReplica/Shared-Nothing semantic matrix remain Gate 5.

Three-backend closure Gate 5A: complete (uncommitted worktree). The feature-gated but non-ignored
`dtgproxy/tests/three_backend_migration.rs` certification runs all six directed pairs:
RocksDB↔PostgreSQL, RocksDB↔Neo4j and PostgreSQL↔Neo4j. Every direction writes the same
cross-partition temporal graph with vertex and edge history, restores through canonical Mapping
export/import, compares the complete ordered canonical snapshot, compares fixed-valid-time current
queries and outbound/inbound adjacency rows, compares deterministic fixed-snapshot Degree results,
and continues at the next durable log index. A real local PostgreSQL 17-compatible server and
Neo4j 5.26 Community instance passed all six directions. Dedicated CI now provisions both services
and runs the certification through the explicit `three-backend-certification` feature.

Three-backend closure Gate 5B: complete (uncommitted worktree). The feature-gated, non-ignored
`gateway-node/tests/three_backend_deployment.rs` test runs the real
Gateway -> DataNode -> TCP Sidecar -> Mapping -> backend path for RocksDB, PostgreSQL and Neo4j in
both PrimaryReplica and Shared-Nothing modes. A disposable PostgreSQL 17 instance and Neo4j 5.26
Community instance passed Temporal Cypher create/count, deterministic two-vertex temporal
transactions, fixed-snapshot Degree, interval components and delta summary with identical ordered
Bolt rows in all six combinations and fixed non-empty semantic expectations for counts, Degree,
components and Delta. PrimaryReplica now runs a two-voter Shard with independent backend instances,
real Data Raft transport, leader election and leader/follower applied-index convergence, while also
certifying the single-Shard transaction fast path;
Shared-Nothing chooses partitions routed to two different Shards and certifies the two-participant
distributed commit path. PostgreSQL backend
construction and Mapping validation run on Tokio's blocking boundary because the synchronous
PostgreSQL client owns an internal runtime. The same CI service job now runs both the six-way
migration certification and this deployment surface matrix. The matrix also fixes Bolt row types
and ordering, verifies two-page Degree reads with `has_more=true` then completion, and compares the
complete normalized code/message for a representative semantic failure across all six
combinations. Gate 6 asynchronous Degree checkpoint/resume, Artifact CAS publication, and
cross-Gateway takeover are implemented and certified across all three backends and both modes.
The same real deployment matrix also exercises asynchronous Degree cancellation on every backend
and deployment mode.
Gate 6 result-reader slice: complete. The canonical `DTAR` result Artifact codec is implemented
and round-trips every current `AlgorithmValue` with CRC, bounds, truncation, corruption and
canonical re-encoding checks. `MetaClusterAnalyticsCoordinator` now accepts an injected
`AnalyticsResultReader`; the Gateway implementation validates the JobSpec graph/schema/topology/
backend fences, resolves the Manifest storage Shard and placement epoch, streams and revalidates
the digest-bound Artifact generation through `RemoteShardClient`, decodes the canonical result,
and returns deterministic offset/limit pages with the total row count.

Gate 6 scheduler slice: substantial implementation complete (uncommitted worktree). Production
Gateway construction now uses configured `node_id` as a stable scheduler Gateway fence. The
scheduler polls Meta claimable jobs, expires old leases before takeover, claims/begins/renews with
CAS revisions, rebuilds a fixed partitioned Snapshot projection, executes Degree, writes and pins
digest-chain checkpoint/result Artifact generations, commits a versioned Degree checkpoint, and
publishes a result only under the current lease fence. A second Gateway takeover test proves an
expired delayed owner is fenced while the new owner publishes and reads the result. Real
RocksDB/PostgreSQL/Neo4j × PrimaryReplica/Shared-Nothing deployment certification now also runs
async Degree submit/status/results through this scheduler. The current Provider SPI is versioned
and fail-closed; built-in Degree/WCC/PageRank expose checkpoint/restore metadata and scheduler
execution renews leases at bounded heartbeat intervals. Real PostgreSQL and Neo4j takeover
certification now covers both PrimaryReplica and Shared-Nothing, with the first owner explicitly
stopped after claim so the second Gateway must cross the lease-expiry fence. Provider-native
execution slices are now part of the current SPI: built-in Degree emits bounded, deterministic
vertex slices and the scheduler assembles them under the lease heartbeat, rejecting a
non-advancing cursor or an incompatible checkpoint cursor. Progress events are now persisted as
successive fenced checkpoint Artifact generations and committed through the Meta CAS path before
the final result is published. The current checkpoint frame also carries the canonical typed
Degree result prefix; takeover validates and restores that prefix and starts the Provider at the
persisted cursor instead of replaying completed slices. Shard GC now permits deletion of a pinned
generation only after a higher generation has advanced the fence; Gateway deletes superseded
checkpoint generations after the Meta CAS commit. The current SPI now carries a generic provider
checkpoint payload: WCC persists deterministic component labels and PageRank persists its rank
vector, iteration, convergence state, graph fingerprint, and parameter fingerprint. Both algorithms
resume through the Gateway scheduler and have ordinary and expired-lease second-Gateway tests with
byte-identical results. Full result retention/orphan scanning and the broader failure-injection
matrix remain open.
Artifact governance now has a pure, deterministic ledger retention planner with validated
generation observations, Meta/pin protection, terminal/orphan TTLs, per-kind generation bounds,
per-job byte bounds, oldest-first reclamation, and reclaimed-byte accounting. Meta protocol now
also exposes a bounded, canonically paginated `ListAnalyticsJobs` maintenance scan carrying
checksum-protected canonical job records. The explicit maintenance path now also includes a
reserved-keyspace generation-head prefix codec and `ListAnalyticsArtifactGenerations` RPC. DataNode
scans heads under a leader read barrier, validates head keys/values and optional pins, and Embedded
and Remote ShardClient implementations expose the same summary contract. Generic Meta scans remain
denied; unpinned generations are reported without fabricating a content manifest. Fenced GC
execution is now invoked by a low-frequency Gateway maintenance tick for generations attached to
Meta job manifests. The scheduler re-reads Meta records, scans the manifest's storage Shard, runs
the deterministic retention planner, and issues only idempotent deletes in the planner output;
both pinned and unpinned observations use the generation head's persisted creation time. Persistent GC lease
ownership remains open, but deletion now has a conservative Meta re-read fence: the scheduler
deletes only when the job revision and complete current Manifest are unchanged and the target
generation is older. Any concurrent job/Manifest mutation aborts the remaining plan. Maintenance
job enumeration is fully paginated instead of inspecting only the first page. Shard-wide orphan
discovery now consumes the current global-head RPC, validates every page boundary, applies the
Meta/revision/GC-epoch fences, and deletes only TTL-eligible generations. The scheduler starts
maintenance after a completed low-frequency interval rather than racing Gateway startup; the
takeover integration test is green with this ordering.
The strict Clippy gate now passes workspace-wide with `cargo clippy --workspace --all-targets --
-D warnings`. The final cleanup moved non-test implementations ahead of the temporal executor test
module instead of suppressing `items_after_test_module`; format and diff checks remain green.
The scheduler now has one current fault-injection SPI covering Claim, Begin, LeaseRenew,
ExecutionSlice, CheckpointUpload, CheckpointPin, CheckpointCas, ResultUpload, ResultPin, and Publish.
Production uses a no-op injector and a deterministic fail-once injector test proves boundary
selection and one-shot behavior. The full process/restart and three-backend injection matrix still
needs to drive this SPI through integration harnesses.
The SPI is now exposed through the current Gateway construction surface for integration harnesses.
It also distinguishes `DTG-ANALYTICS-FAULT-PROCESS-STOP` from an ordinary injected operation
failure: a simulated Gateway stop is not converted into a terminal Job FAILED record, leaving the
lease/checkpoint state available for expiry and takeover. The real deployment harness now injects
a fail-once process stop at the Begin boundary for PostgreSQL and Neo4j takeover runs in both
PrimaryReplica and Shared-Nothing modes; the test compiles with the three-backend certification
feature. Targeted scheduler tests and strict Gateway Clippy remain green. Result byte-identity and
the complete per-boundary × backend × deployment restart matrix remain open.
GC fencing now has a current end-to-end epoch path. Meta exposes a dedicated analytics GC lease
RPC; a Gateway must acquire the lease before maintenance and re-check its expiry before every
delete. The Meta response carries a term-derived, monotonically increasing `gc_epoch`, which is
propagated through the current Shard client, DataNode RPC, and `DeleteAnalyticsArtifactGeneration`
Raft command. Each Shard persists the highest accepted epoch in its reserved analytics keyspace,
so a delayed lower-epoch Gateway is rejected after a newer owner has performed a delete. The
embedded and remote Shard TCKs, Meta lease test, Raft command codec test, Shard artifact state
machine tests, and target-package strict Clippy pass. The GC lease owner, owner term, epoch,
expiration and command identity are now committed through a dedicated Meta Raft command and are
encoded in the current Meta snapshot/journal state. A process-level restart test proves that a new
Meta term restores the committed lease and advances the GC epoch before granting ownership to a
different Gateway. Full concurrent-Gateway GC certification remains open; scheduler metrics now
expose maintenance failures, orphan discovery/protection/TTL skips, deletes, reclaimed bytes, and
checkpoint-resume counts through the Gateway service. Latest pinned orphan generations belonging to
terminal Jobs now use an explicit GC-epoch-fenced Shard fence-advance command before deletion;
non-terminal or unknown latest pinned generations remain conservatively protected.
Artifact generation creation time is now a persisted, checksum-protected property of the current
generation head. The Gateway supplies one non-zero `created_at_unix_ms` for all chunks, the Raft
command and DataNode/Embedded/Remote paths preserve it, and a later chunk or replay with a different
time is rejected as a conflict. Old, truncated and corrupt head/command encodings fail closed. A
Gateway retry now scans the existing generation head before upload and reuses its creation time;
non-canonical scans and a saturated 4096-entry page that cannot prove absence fail closed. Retention
uses the persisted generation time for both pinned and unpinned generations and no longer substitutes
the Job submission time or maintenance scan time. Focused protocol/runtime/RPC/client tests, the
five-package 163-test regression, Gateway tests, six-package strict Clippy, format and diff checks
pass; independent re-review found no Critical or Important issues. Shard-wide discovery, orphan
TTL planning, and terminal-Job latest-pinned fence advancement are now implemented. Real
three-backend concurrent-Gateway certification remains open; unknown/non-terminal latest pinned
generations intentionally remain protected until a Meta tombstone protocol can prove quiescence.
The Shard-wide discovery transport is now implemented as a separate current-only
`ListAnalyticsArtifactGenerationHeads` RPC; the existing job/kind filtered list remains intact for
targeted retry and retention. The new API uses a bounded structured `(job_id, kind, generation)`
cursor, exclusive canonical head-key order, and a final empty page after a full page. Runtime,
DataNode, Embedded and Remote paths validate reserved-prefix identity, head/pin provenance,
unpinned manifest absence, applied-index stability, and malformed cursors/responses fail-closed.
Both filtered and global DataNode scans now use a status-fenced stable snapshot around head scan and
pin multi-get; Remote filtered/global pages share the strict row decoder. Four-package regression
now passes 160/160 with strict Clippy, fmt and diff checks. Gateway orphan discovery and TTL delete
planning now consume this page API; the serial PrimaryReplica takeover matrix passes 3/3, while
parallel test execution remains intentionally unsupported because fixtures share process-local
resources.

Three-backend resumable analytics fault certification is now complete for the current scheduler
boundaries (uncommitted worktree). The real deployment harness runs Degree, WCC and PageRank on
RocksDB, PostgreSQL 17 and Neo4j 5.26 Community in both PrimaryReplica and Shared-Nothing modes.
All three algorithms cross the `Begin` process-stop boundary; Degree additionally crosses Claim,
LeaseRenew, ExecutionSlice, CheckpointUpload, CheckpointPin, CheckpointCas, ResultUpload, ResultPin
and Publish. Every case is recovered by a second Gateway and compares the complete pinned canonical
`DTAR` Result Artifact bytes with an uninterrupted baseline after validating its manifest byte count
and BLAKE3 digest. All six backend-by-mode jobs pass the complete 13-case suite.
Remote Shard `read_keys` and `scan` now retry alternate replicas during transient Leader changes;
a dedicated TCK reproduces a stale Catalog Leader and passes together with the full Remote TCK
(4/4). PostgreSQL/Neo4j certification runs use a fresh backend namespace per fixture, preventing
durable applied-index state from being rebound to a new Raft log. CI is split into a migration/
surface job and six parallel backend-by-mode takeover jobs. Checkpoint recovery now preserves the
first projection's input fence across Artifact-only applied-index changes, and skips occupied orphan
generations after upload/pin/CAS interruption instead of overwriting them. Gateway lib 33/33 and
Remote TCK 4/4 pass. A same-stable-ID Gateway runtime restart at ExecutionSlice is also certified
across all six backend-by-mode jobs: restart must wait for a new lease epoch and produces the same
Result Artifact bytes, so stable Gateway identity cannot bypass fencing. OS-process Gateway restart
is now certified with the same stable node ID: an in-flight Degree job is killed at `RUNNING`, the
restarted process obtains a new lease epoch, and the final Shard contains exactly one pinned Result
generation. A separate three-node Meta quorum test stops the active Leader while the job is in
flight; the surviving majority elects a new Leader, the Gateway switches Meta endpoints with
bounded per-attempt deadlines and recent-Leader affinity, and the job publishes one pinned Result.
Polling uses unique request IDs so request-level idempotency cannot mask state transitions. Full
Shard/backend and combined process-restart sequences remain open. Existing component process
baselines are freshly green: Meta restart 2/2
preserves timestamp leases and advances the committed analytics GC epoch; DataNode restart 1/1
restores the durable Replica, request deduplication and pinned Artifact bytes. These component tests
do not yet prove an in-flight analytics Job across a combined restart sequence.

Analytics Job tombstone reclamation is now implemented in the current format. Terminal prune
atomically moves a Job into a durable Ledger tombstone; command and snapshot codecs, revision/epoch
fences, acknowledgement and compaction invariants pass the Analytics Ledger suite. Meta exposes
checksum-protected canonical tombstone pagination and rejects acknowledgement unless Gateway ID,
GC epoch and lease expiry match the committed lease. A real Meta child-process restart preserves an
unacknowledged tombstone and advances the recovered GC epoch. Gateway orphan maintenance now joins
active Jobs, tombstones and complete Shard-wide head scans: active records override duplicates,
unknown Jobs are fail-closed, and only an unreclaimed tombstone permits latest-pinned fence advance
and deletion. A process integration test deletes a tombstoned pinned Result, kills the first Gateway
before acknowledgement, lets a different Gateway acknowledge after a later empty scan, and proves
unknown and non-terminal Job Artifacts remain. The independent fence-advance-before-delete process
crash injection and real three-backend concurrent-GC matrix are now closed by the later
backend-by-mode certification described below.

In-flight DataNode/Shard restart recovery is now implemented and RocksDB integration-certified for
PrimaryReplica and Shared-Nothing. A controlled TCP boundary closes every existing HTTP/2
connection, the DataNode is
reopened with the same durable directory, node/shard/placement/backend identity and fixed Gateway
address, while Gateway and Meta remain alive. The scheduler no longer writes a terminal `FAILED`
record for typed Shard transport, Leader, deadline, ReadIndex or storage-unavailable failures.
`Unavailable` is preserved through Remote Shard, `StorageAdapter`, analytics projection and Sidecar
transport instead of being flattened into a generic projection string; deterministic corruption and
stale-epoch failures remain fail-closed. The recovery test compares the complete canonical `DTAR`
Result bytes with an uninterrupted baseline and proves exactly one pinned Result generation. It
passed once after the red/green cycle and three consecutive PrimaryReplica repeat runs; the complete
five-test two-mode analytics suite is green. Backend Sidecar restart is now real-backend certified
for RocksDB, PostgreSQL 17 and Neo4j 5.26 Community in both deployment modes. Each case stops the
Sidecar after Degree reaches `RUNNING`, keeps its backend root/instance identity and advertised
address fixed, proves the outage is non-terminal, reopens the same backend, and compares the pinned
canonical `DTAR` bytes with an uninterrupted baseline. A second six-case matrix destroys the first
Gateway during the outage and requires a distinct Gateway ID to claim a higher lease epoch after
the Sidecar returns; every case ends with exactly one pinned Result generation. Isolated
DataNode-only restart remains RocksDB-only. Ordered Gateway + Meta + all Shard/backend restart is
now real-backend certified for RocksDB, PostgreSQL 17 and Neo4j 5.26 Community in both deployment
modes. Every case preserves Meta journal/state, DataNode durable roots, backend root/instance
identity and fixed addresses, destroys the old Gateway, and requires a different Gateway to take
over after restart. All six cases produce baseline-identical `DTAR` bytes, exactly one pinned Result
generation and no ghost generation. PrimaryReplica now uses two voters with separate backend
instances, stops/reopens both, and verifies leader/follower applied-index convergence before and
after takeover. The RocksDB predecessor exposed and fixed a cross-process TSO
idempotency collision: Gateway Bolt/Meta request IDs include a per-process-instance nonce, while the
stable Gateway ID remains reserved for lease fencing.

Tombstone GC crash recovery is now real-backend certified across RocksDB, PostgreSQL 17 and Neo4j
5.26 Community in PrimaryReplica and Shared-Nothing. The 18-case matrix independently stops the old
Gateway after fence advance, before delete, or before acknowledgement. Two replacement Gateways
then compete for the durable Meta GC lease; scheduler metrics require at least one observed
`ResourceExhausted` non-owner rejection, the global
successful-delete count remains exactly one, the next complete empty scan acknowledges the
tombstone, and Unknown/non-terminal pinned Artifacts remain fail-closed. The three-backend CI now
runs both this matrix and the six-case ordered full-stack restart matrix per backend/mode job.
GC maintenance renews the current owner epoch during long Meta/Shard pagination and forcibly
revalidates the lease immediately before fence advance, delete and acknowledgement; each Shard RPC
deadline is capped by the committed Meta lease expiry.

DTGProxy 1.1 seven-condition audit (2026-07-23):
1. WCC/PageRank native versioned checkpoint state, deterministic slices, direct cross-Gateway
   restore and byte-identical recovery are implemented and covered by Provider/Gateway tests.
2. The unified AnalyticsProvider SPI exposes checkpoint/restore/slice lifecycle for Degree, WCC and
   PageRank; unsupported algorithms fail closed with a stable provider error instead of silently
   recomputing.
3. Result/Checkpoint retention, terminal tombstones, orphan discovery, generation/byte/TTL limits,
   reclaimed-byte metrics, GC lease fencing and restart recovery are implemented and certified.
4. Claim, Begin, LeaseRenew, ExecutionSlice, CheckpointUpload, CheckpointPin, CheckpointCas,
   ResultUpload, ResultPin, Publish and Gateway restart boundaries are covered across all three
   backends and both deployment modes; the GC matrix separately covers fence/delete/ack crashes.
5. Gateway process restart, Meta leader change, DataNode/Shard restart, backend Sidecar restart and
   ordered full-stack restart recover without duplicate publication or ghost generations.
6. Local quality gates are green: format, diff check, Gateway three-backend strict Clippy, workspace
   strict Clippy, Provider, Ledger, Cluster Protocol, Meta and Gateway recovery suites. The real
   backend 18-case GC matrix and six-case full-stack matrix are green and wired into CI. A hosted CI
   run cannot cover the current uncommitted worktree yet, so the Goal remains active on that external
   evidence boundary.
7. The current design specification, Adapter SPI, backend migration runbook and progress ledger use
   explicit implemented/compiled/integration-certified/real-backend-certified/production-certified
   vocabulary and retain no old V2 compatibility path.

Final gate debugging also removed a flaky Meta quorum assumption: after isolating node 1, the test
now accepts whichever surviving voter (2 or 3) Raft legally elects and drives the remaining fencing
checks through that Leader. The old hard-coded node-2 assertion reproduced a legal node-3 election;
the corrected test passed 20/20 repetitions and the complete serial Meta suite.

Current validation refresh (2026-07-23): a `Publish` crash after Result upload/pin and before Meta
Manifest CAS now reuses an existing pinned Result generation only on exact chunk-count, total-byte
and BLAKE3 content-digest equality; a saturated bounded scan fails closed. The focused selector test
and the real RocksDB PrimaryReplica Publish recovery case pass. The complete 588.32-second
three-backend resumable matrix then passed across RocksDB, PostgreSQL 17 and Neo4j 5.26 Community in
PrimaryReplica and Shared-Nothing: Degree covers Claim, Begin, LeaseRenew, ExecutionSlice,
CheckpointUpload, CheckpointPin, CheckpointCas, ResultUpload, ResultPin and Publish, while WCC and
PageRank cover Begin takeover; every recovered `DTAR` is baseline-byte-identical with one pinned
Result. Gateway lib (42), Gateway process (4), Gateway PrimaryReplica/Shared-Nothing recovery (7),
Gateway service (1), Analytics Runtime, Analytics Ledger, Cluster Protocol, Shard Client (Embedded
8 and Remote 4), and Meta Node suites were rerun successfully. `cargo fmt --all -- --check`,
`git diff --check`, Gateway strict Clippy with `three-backend-certification`, and workspace strict
Clippy all pass. The Embedded Shard TCK now keeps fence-advance/delete assertions with the test that
creates and pins the generation; the generic-bypass test remains an empty-state rejection test.
Hosted CI still cannot attest to this uncommitted worktree, so the Goal remains active solely on that
external production-certification evidence boundary.
