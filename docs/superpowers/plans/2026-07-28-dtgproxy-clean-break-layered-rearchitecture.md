# DTGProxy Clean-Break Layered Rearchitecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Replace the current DTGProxy runtime with the approved minimal-kernel, language,
execution, and storage architecture; cut the four thin processes over to it; certify every required
behavior; and delete the complete legacy runtime dependency closure.

**Architecture:** Build a clean-room dependency tree beside the old code with zero new-to-old
imports. Establish contracts from the bottom upward, port validated behavior behind new types,
switch Gateway/Data/Meta/Controller only after the new path passes distributed certification, then
delete the old crates and compatibility surfaces in one final gate.

**Tech Stack:** Rust 1.93.0, Cargo resolver 3, Tokio 1.53.0, Tonic 0.14.6, Prost 0.14.4, raft-rs
0.7.0, Fjall 3.1.8, tokio-postgres 0.7.18, Neo4j Query API, Blake3 1.8.5, Serde 1.0.228.

## Global Constraints

- The approved design is docs/superpowers/specs/2026-07-28-dtgproxy-clean-break-layered-architecture-design.md.
- The current dirty worktree is authoritative; preserve unrelated edits and never reconstruct the
  implementation from HEAD alone.
- New crates may coexist with old crates during development, but no new crate may depend on an old
  runtime crate.
- Dependencies point only process -> execution/provider, execution -> language/storage/kernel,
  language -> kernel, and storage/provider -> kernel/storage contract.
- Kernel has no async runtime, I/O, RPC, Raft, storage trait, query plan, backend capability,
  database driver, or process role.
- Language output stops at the new normalized logical IR.
- Execution owns planning, query, Temporal Snapshot Isolation, Shard/Raft, consistent reads,
  replica snapshots, Snapshot CSR, built-in algorithms, asynchronous analytics, control state, and
  backend migration.
- Storage exposes typed logical contracts; canonical KV is not a runtime interchange contract.
- Official Fjall, PostgreSQL, and Neo4j providers run in-process.
- Third-party storage uses only the versioned remote storage protocol.
- A placement epoch has one active backend generation; generation numbers never decrease or repeat.
- Every replica namespace is exclusive and ownership-fenced.
- Every Raft group is homogeneous within a backend generation.
- One Data Node may host Shards backed by different official or remote providers.
- Arbitrary user procedures or code execution are forbidden.
- Old disk, WAL, snapshot, IR, and internal RPC formats are rejected by the new runtime.
- Every implementation task follows red-green-refactor, runs its exact focused tests, runs
  cargo fmt --check for touched Rust code, and commits only its own files.

---

## Target file map

### Kernel

- crates/kernel/dtg-kernel/src/lib.rs: facade and dependency allowlist.
- crates/kernel/dtg-kernel/src/id.rs: strongly typed identifiers.
- crates/kernel/dtg-kernel/src/time.rs: transaction time and valid interval algebra.
- crates/kernel/dtg-kernel/src/value.rs: backend-neutral scalar/property values.
- crates/kernel/dtg-kernel/src/version.rs: explicit version and digest types.
- crates/kernel/dtg-kernel/src/error.rs: shared error and retry classes.

### Language

- crates/language/dtg-language-ir/src/: new normalized logical IR and validator.
- crates/language/dtg-language/src/: lexer, parser, semantic analysis, normalization, compiler.

### Storage

- crates/storage/dtg-storage/src/: binding, capability, mutation, read-view, snapshot, consensus,
  artifact, and error contracts.
- crates/storage/dtg-storage-fjall/src/: Fjall graph, consensus, snapshot, and artifact stores.
- crates/storage/dtg-storage-postgres/src/: native PostgreSQL schema and provider.
- crates/storage/dtg-storage-neo4j/src/: native Neo4j model and provider.
- crates/storage/dtg-storage-remote-protocol/: versioned third-party protocol.
- crates/storage/dtg-storage-remote/src/: remote client and reference conformance server.

### Execution

- crates/execution/dtg-plan/src/: logical optimization, routing, physical plan, fragmentation.
- crates/execution/dtg-query/src/: columnar operators, exchanges, budgets, residual evaluation.
- crates/execution/dtg-transaction/src/: snapshot tokens, conflict checks, 1PC/2PC, recovery.
- crates/execution/dtg-shard/src/: Raft state machine, read barriers, replica snapshots.
- crates/execution/dtg-analytics/src/: Snapshot CSR, built-in algorithms, durable jobs.
- crates/execution/dtg-control/src/: catalog, placements, bindings, reconciliation, migration.
- crates/execution/dtg-cluster-protocol/: new incompatible internal cluster protocol.
- crates/execution/dtg-execution/src/: stable execution facade and composition APIs.

### Processes and gates

- crates/processes/dtg-gateway/: thin Bolt/T-Cypher process.
- crates/processes/dtg-data/: thin heterogeneous ShardHost process.
- crates/processes/dtg-meta/: thin Meta-Raft process.
- crates/processes/dtg-controller/: thin reconciliation process.
- scripts/check-layered-architecture.sh: dependency and forbidden-edge gate.
- scripts/check-clean-break-removal.sh: final legacy-removal gate.
- scripts/certify-clean-break.sh: complete local certification entry point.

---

### Task 1: Establish the clean-room workspace and dependency gate

**Files:**
- Create: all target Cargo.toml files and minimal src/lib.rs facades listed in the target file map.
- Create: scripts/check-layered-architecture.sh
- Create: scripts/tests/check-layered-architecture-contract.sh
- Modify: Cargo.toml
- Test: scripts/tests/check-layered-architecture-contract.sh

**Interfaces:**
- Consumes: the root workspace package metadata and lints.
- Produces: named empty facade crates and a machine-enforced dependency allowlist used by every
  later task.

- [ ] **Step 1: Write the failing architecture contract**

~~~bash
#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/check-layered-architecture.sh
scripts/check-layered-architecture.sh

for package in dtg-kernel dtg-language-ir dtg-language dtg-storage dtg-storage-fjall dtg-storage-postgres dtg-storage-neo4j dtg-storage-remote-protocol dtg-storage-remote dtg-plan dtg-query dtg-transaction dtg-shard dtg-analytics dtg-control dtg-cluster-protocol dtg-execution dtg-gateway dtg-data dtg-meta dtg-controller
do
  cargo metadata --no-deps --format-version 1 |
    jq -e --arg package "$package" '.packages[] | select(.name == $package)' >/dev/null
done
~~~

- [ ] **Step 2: Run the contract and verify red**

Run: bash scripts/tests/check-layered-architecture-contract.sh

Expected: FAIL because scripts/check-layered-architecture.sh and the new packages do not exist.

- [ ] **Step 3: Create the crate skeletons and dependency checker**

Each facade starts with this exact boundary marker:

~~~rust
#![forbid(unsafe_code)]

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
~~~

The checker reads cargo metadata and rejects:

~~~text
dtg-kernel -> any dtg-* package
dtg-language* -> dtg-execution*, dtg-storage*, or any process
dtg-storage* -> dtg-language*, dtg-execution*, or any process
dtg-execution* -> any process or concrete storage provider
any new package -> adapter-*, storage-api, temporal-ir, physical-plan,
                   query-executor, query-optimizer, distributed-query,
                   procedure-runtime, cypher-engine, temporal-storage,
                   raft-logstore, cluster-protocol, data-node, gateway-node,
                   meta-node, controller, or dtgproxy
~~~

Root Cargo.toml lists both trees temporarily and uses resolver 3.

- [ ] **Step 4: Run the contract and workspace checks**

Run: bash scripts/tests/check-layered-architecture-contract.sh

Expected: PASS.

Run: cargo check -p dtg-kernel -p dtg-language-ir -p dtg-storage -p dtg-execution

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add Cargo.toml crates/kernel crates/language crates/storage crates/execution crates/processes scripts/check-layered-architecture.sh scripts/tests/check-layered-architecture-contract.sh
git commit -m "build: establish clean-break architecture boundaries"
~~~

### Task 2: Implement the minimal kernel

**Files:**
- Modify: crates/kernel/dtg-kernel/Cargo.toml
- Create: crates/kernel/dtg-kernel/src/id.rs
- Create: crates/kernel/dtg-kernel/src/time.rs
- Create: crates/kernel/dtg-kernel/src/value.rs
- Create: crates/kernel/dtg-kernel/src/version.rs
- Create: crates/kernel/dtg-kernel/src/error.rs
- Modify: crates/kernel/dtg-kernel/src/lib.rs
- Test: crates/kernel/dtg-kernel/tests/kernel_contract.rs

**Interfaces:**
- Produces: ClusterId, GraphId, ShardId, ReplicaId, PlacementEpoch, BackendGeneration,
  TransactionId, TransactionTime, ValidInterval, Value, Version, Digest32, ErrorClass, RetryClass.

- [ ] **Step 1: Write failing value and identity tests**

~~~rust
use dtg_kernel::{
    BackendGeneration, GraphId, PlacementEpoch, ShardId, TransactionTime, ValidInterval,
};

#[test]
fn zero_and_reversed_values_are_rejected() {
    assert!(GraphId::new(0).is_err());
    assert!(ShardId::new(0).is_err());
    assert!(PlacementEpoch::new(0).is_err());
    assert!(BackendGeneration::new(0).is_err());
    assert!(ValidInterval::new(20, 10).is_err());
}

#[test]
fn adjacent_intervals_do_not_overlap() {
    let left = ValidInterval::new(10, 20).unwrap();
    let right = ValidInterval::new(20, 30).unwrap();
    assert!(!left.overlaps(right));
    assert_eq!(TransactionTime::new(9).unwrap().get(), 9);
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-kernel --test kernel_contract

Expected: FAIL with unresolved kernel types.

- [ ] **Step 3: Implement checked newtypes and interval algebra**

~~~rust
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct GraphId(u64);

impl GraphId {
    pub fn new(value: u64) -> Result<Self, KernelError> {
        (value != 0).then_some(Self(value)).ok_or(KernelError::ZeroIdentifier)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidInterval {
    start: i64,
    end: i64,
}

impl ValidInterval {
    pub fn new(start: i64, end: i64) -> Result<Self, KernelError> {
        (start < end)
            .then_some(Self { start, end })
            .ok_or(KernelError::InvalidInterval { start, end })
    }

    pub const fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}
~~~

Use the same nonzero pattern for all identifiers. Value supports Null, Boolean, Integer, FloatBits,
Bytes, String, List, and Map with total deterministic ordering.

- [ ] **Step 4: Enforce kernel dependency purity**

Run: bash scripts/check-layered-architecture.sh

Expected: PASS and report zero forbidden kernel dependencies.

Run: cargo test -p dtg-kernel

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add crates/kernel/dtg-kernel
git commit -m "feat: add minimal DTG kernel types"
~~~

### Task 3: Define the new normalized logical IR

**Files:**
- Modify: crates/language/dtg-language-ir/Cargo.toml
- Create: crates/language/dtg-language-ir/src/expr.rs
- Create: crates/language/dtg-language-ir/src/schema.rs
- Create: crates/language/dtg-language-ir/src/plan.rs
- Create: crates/language/dtg-language-ir/src/analytics.rs
- Create: crates/language/dtg-language-ir/src/validate.rs
- Modify: crates/language/dtg-language-ir/src/lib.rs
- Test: crates/language/dtg-language-ir/tests/normalized_ir.rs

**Interfaces:**
- Consumes: dtg-kernel identifiers, intervals, times, and values.
- Produces: LogicalProgram, LogicalStatement, LogicalPlan, LogicalExpr, TemporalScope,
  BuiltInAlgorithmId, AnalyticsSubmission, validate_program.

- [ ] **Step 1: Write failing boundary tests**

~~~rust
use dtg_language_ir::{
    validate_program, BuiltInAlgorithmId, IrVersion, LogicalPlan, LogicalProgram, LogicalStatement,
};

#[test]
fn current_ir_has_one_fresh_major_version() {
    assert_eq!(IrVersion::CURRENT.major(), 1);
}

#[test]
fn analytics_accepts_only_closed_built_in_ids() {
    assert!(BuiltInAlgorithmId::try_from("page_rank").is_ok());
    assert!(BuiltInAlgorithmId::try_from("user.uploaded_code").is_err());
}

#[test]
fn empty_query_is_rejected() {
    let program = LogicalProgram::new(LogicalStatement::Query(LogicalPlan::empty()));
    assert!(validate_program(&program).is_err());
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-language-ir --test normalized_ir

Expected: FAIL with unresolved IR types.

- [ ] **Step 3: Implement the normalized IR surface**

~~~rust
pub struct LogicalProgram {
    pub version: IrVersion,
    pub parameters: Vec<Parameter>,
    pub statement: LogicalStatement,
    pub result_schema: RowSchema,
}

pub enum LogicalStatement {
    Query(LogicalPlan),
    Write(LogicalWrite),
    SubmitAnalytics(AnalyticsSubmission),
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
}

pub struct LogicalPlan {
    pub root: LogicalNodeId,
    pub nodes: Vec<LogicalNode>,
}

pub enum TimeExpr {
    Literal(TransactionTime),
    Parameter(String),
}

pub enum TemporalScope {
    Current,
    AsOf(TimeExpr),
    Changes { from: TimeExpr, to: TimeExpr },
}
~~~

LogicalNode contains typed scan, expand, filter, project, join, aggregate, sort, limit, unwind, and
subquery nodes. It contains no Shard, provider, capability, exchange, physical operator, procedure
provider, or old-plan compatibility variant.

- [ ] **Step 4: Add forbidden-surface tests**

Run: cargo doc -p dtg-language-ir --no-deps

Expected: PASS.

Run: if rg -n "ShardId|BackendGeneration|Capability|Exchange|ProcedureProvider|temporal_ir" crates/language/dtg-language-ir/src; then exit 1; fi

Expected: no matches.

Run: cargo test -p dtg-language-ir

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add crates/language/dtg-language-ir
git commit -m "feat: define normalized T-Cypher logical IR"
~~~

### Task 4: Build the T-Cypher language frontend

**Files:**
- Modify: crates/language/dtg-language/Cargo.toml
- Create: crates/language/dtg-language/src/token.rs
- Create: crates/language/dtg-language/src/lexer.rs
- Create: crates/language/dtg-language/src/ast.rs
- Create: crates/language/dtg-language/src/parser.rs
- Create: crates/language/dtg-language/src/sema.rs
- Create: crates/language/dtg-language/src/normalize.rs
- Modify: crates/language/dtg-language/src/lib.rs
- Test: crates/language/dtg-language/tests/compile.rs
- Test: crates/language/dtg-language/tests/no_user_procedures.rs

**Interfaces:**
- Consumes: T-Cypher text, parameter declarations, SchemaCatalog.
- Produces: Language and compile(source, catalog) -> Result<LogicalProgram, LanguageError>.

- [ ] **Step 1: Write failing compilation tests**

~~~rust
use dtg_language::{compile, EmptySchemaCatalog};
use dtg_language_ir::{LogicalStatement, TemporalScope, TimeExpr};

#[test]
fn as_of_query_normalizes_to_explicit_scope() {
    let program = compile(
        "MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert!(plan.contains_scope(TemporalScope::AsOf(TimeExpr::Parameter("t".into()))));
}

#[test]
fn arbitrary_call_is_rejected_at_compile_time() {
    let error = compile("CALL user.code()", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNKNOWN-BUILTIN");
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-language --test compile --test no_user_procedures

Expected: FAIL because compile and the new parser do not exist.

- [ ] **Step 3: Implement the compiler pipeline**

~~~rust
pub fn compile(
    source: &str,
    catalog: &dyn SchemaCatalog,
) -> Result<dtg_language_ir::LogicalProgram, LanguageError> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(&tokens)?;
    let typed = sema::analyze(ast, catalog)?;
    let program = normalize::normalize(typed)?;
    dtg_language_ir::validate_program(&program)?;
    Ok(program)
}

pub struct Language {
    catalog: std::sync::Arc<dyn SchemaCatalog>,
}

impl Language {
    pub fn compile(&self, source: &str) -> Result<dtg_language_ir::LogicalProgram, LanguageError> {
        compile(source, self.catalog.as_ref())
    }
}
~~~

Port syntax behavior only through new AST and IR types. Recognize the static built-in analytics
catalog and asynchronous submission syntax; delete any fallback lookup path from the new frontend.

- [ ] **Step 4: Run language tests and old/new independence scan**

Run: cargo test -p dtg-language

Expected: PASS.

Run: cargo tree -p dtg-language

Expected: only dtg-language-ir and dtg-kernel among DTGProxy packages.

- [ ] **Step 5: Commit**

~~~bash
git add crates/language/dtg-language
git commit -m "feat: compile T-Cypher into normalized logical IR"
~~~

### Task 5: Define the typed storage contract family

**Files:**
- Modify: crates/storage/dtg-storage/Cargo.toml
- Create: crates/storage/dtg-storage/src/binding.rs
- Create: crates/storage/dtg-storage/src/capability.rs
- Create: crates/storage/dtg-storage/src/mutation.rs
- Create: crates/storage/dtg-storage/src/read.rs
- Create: crates/storage/dtg-storage/src/snapshot.rs
- Create: crates/storage/dtg-storage/src/consensus.rs
- Create: crates/storage/dtg-storage/src/artifact.rs
- Create: crates/storage/dtg-storage/src/error.rs
- Create: crates/storage/dtg-storage/src/tck.rs
- Modify: crates/storage/dtg-storage/src/lib.rs
- Test: crates/storage/dtg-storage/tests/contracts.rs

**Interfaces:**
- Produces: ReplicaBinding, BackendClass, CapabilityManifest, CommittedShardBatch,
  ReplicaStateStore, TemporalReadView, LogicalSnapshotSource, LogicalSnapshotSink,
  PushdownExecutor, ConsensusStore, ArtifactStore, run_storage_tck.

- [ ] **Step 1: Write failing binding and capability tests**

~~~rust
use dtg_storage::{
    BackendClass, CapabilityManifest, ProviderKind, ReplicaBinding, StorageError,
};

#[test]
fn binding_rejects_zero_epoch_and_generation() {
    assert!(matches!(
        ReplicaBinding::builder().placement_epoch(0).build(),
        Err(StorageError::InvalidBinding(_))
    ));
}

#[test]
fn capability_manifest_digest_is_order_independent() {
    let a = CapabilityManifest::from_names(["point", "adjacency"]).unwrap();
    let b = CapabilityManifest::from_names(["adjacency", "point"]).unwrap();
    assert_eq!(a.digest(), b.digest());
}

#[test]
fn backend_class_excludes_endpoint_but_includes_layout() {
    let class = BackendClass::new(ProviderKind::Fjall, 1, 3, ["point"]).unwrap();
    assert_ne!(class.digest(), BackendClass::new(ProviderKind::Fjall, 1, 4, ["point"]).unwrap().digest());
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-storage --test contracts

Expected: FAIL with unresolved contract types.

- [ ] **Step 3: Implement object-safe async contracts**

~~~rust
pub type StoreFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, StorageError>> + Send + 'a>>;

pub trait ReplicaStateStore: Send + Sync {
    fn binding(&self) -> &ReplicaBinding;
    fn applied_index(&self) -> StoreFuture<'_, u64>;
    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt>;
    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>>;
}

pub trait TemporalReadView: Send + Sync {
    fn fence(&self) -> &ReadFence;
    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>>;
    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>>;
    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>>;
    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage>;
}
~~~

CommittedShardBatch contains typed logical mutations and no LogicalKey or raw key/value fields.
Every contract method receives or owns the complete ReplicaBinding identity.

- [ ] **Step 4: Add a deterministic in-memory TCK store under cfg(test)**

Run: cargo test -p dtg-storage

Expected: PASS with atomicity, monotonic index, idempotent replay, stale binding, immutable read
view, capability, snapshot, and namespace tests.

Run: if rg -n "StorageAdapter|TemporalBackendMapping|LogicalKey|canonical.kv" crates/storage/dtg-storage; then exit 1; fi

Expected: no matches.

- [ ] **Step 5: Commit**

~~~bash
git add crates/storage/dtg-storage
git commit -m "feat: define typed logical storage contracts"
~~~

### Task 6: Implement the Fjall graph and consensus stores

**Files:**
- Modify: crates/storage/dtg-storage-fjall/Cargo.toml
- Create: crates/storage/dtg-storage-fjall/src/namespace.rs
- Create: crates/storage/dtg-storage-fjall/src/codec.rs
- Create: crates/storage/dtg-storage-fjall/src/graph.rs
- Create: crates/storage/dtg-storage-fjall/src/read_view.rs
- Create: crates/storage/dtg-storage-fjall/src/snapshot.rs
- Create: crates/storage/dtg-storage-fjall/src/consensus.rs
- Create: crates/storage/dtg-storage-fjall/src/artifact.rs
- Modify: crates/storage/dtg-storage-fjall/src/lib.rs
- Test: crates/storage/dtg-storage-fjall/tests/storage_tck.rs
- Test: crates/storage/dtg-storage-fjall/tests/consensus_recovery.rs
- Test: crates/storage/dtg-storage-fjall/tests/namespace_isolation.rs

**Interfaces:**
- Consumes: dtg-storage contracts.
- Produces: FjallReplicaStore::open, FjallConsensusStore::open, FjallArtifactStore::open.

- [ ] **Step 1: Write failing namespace and recovery tests**

~~~rust
#[test]
fn mismatched_binding_cannot_reopen_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let first = fixture_binding(1, 1);
    FjallReplicaStore::open(dir.path(), first).unwrap();
    let error = FjallReplicaStore::open(dir.path(), fixture_binding(2, 1)).unwrap_err();
    assert_eq!(error.code(), "DTG-STORAGE-NAMESPACE-OWNER");
}

#[test]
fn consensus_entries_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = FjallConsensusStore::open(dir.path(), fixture_replica()).unwrap();
    store.append(vec![entry(4), entry(5)]).unwrap();
    drop(store);
    let reopened = FjallConsensusStore::open(dir.path(), fixture_replica()).unwrap();
    assert_eq!(reopened.entries(4, 6, 1024).unwrap().len(), 2);
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-storage-fjall

Expected: FAIL because Fjall stores are not implemented.

- [ ] **Step 3: Implement exclusive namespaces and native keyspaces**

Use Fjall 3.1.8 partitions named owner, identity, current_vertex, current_edge, history,
adjacency_out, adjacency_in, temporal_index, transaction, replica_meta, snapshot_stage, artifact,
raft_log, raft_state, and raft_snapshot. Persist the complete binding owner record before opening
business partitions. Apply one CommittedShardBatch in one Fjall batch and update applied_index in
the same durable commit.

- [ ] **Step 4: Implement immutable reads, snapshots, consensus, and artifacts**

~~~rust
impl ReplicaStateStore for FjallReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { self.apply_sync(batch) })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            self.verify_fence(&fence)?;
            Ok(Box::new(self.snapshot_view(fence)?) as Box<dyn TemporalReadView>)
        })
    }
}
~~~

- [ ] **Step 5: Run provider and architecture checks**

Run: cargo test -p dtg-storage-fjall

Expected: PASS.

Run: bash scripts/check-layered-architecture.sh

Expected: PASS.

Run: if cargo tree -p dtg-storage-fjall | rg rocksdb; then exit 1; fi

Expected: no matches.

- [ ] **Step 6: Commit**

~~~bash
git add crates/storage/dtg-storage-fjall
git commit -m "feat: add Fjall graph and consensus storage"
~~~

### Task 7: Implement the deterministic Shard/Raft runtime

**Files:**
- Modify: crates/execution/dtg-shard/Cargo.toml
- Create: crates/execution/dtg-shard/src/command.rs
- Create: crates/execution/dtg-shard/src/state_machine.rs
- Create: crates/execution/dtg-shard/src/raft_store.rs
- Create: crates/execution/dtg-shard/src/replica.rs
- Create: crates/execution/dtg-shard/src/host.rs
- Modify: crates/execution/dtg-shard/src/lib.rs
- Test: crates/execution/dtg-shard/tests/determinism.rs
- Test: crates/execution/dtg-shard/tests/recovery.rs

**Interfaces:**
- Consumes: dtg-kernel, dtg-storage, Fjall ConsensusStore, raft-rs.
- Produces: ShardCommand, ShardStateMachine, RaftReplica, ShardHost, ProposalReceipt.

- [ ] **Step 1: Write failing deterministic-apply tests**

~~~rust
#[test]
fn replaying_one_command_is_idempotent() {
    let mut machine = fixture_machine();
    let command = committed_vertex_command(7);
    let first = machine.apply_committed(11, 7, command.clone()).unwrap();
    let second = machine.apply_committed(11, 7, command).unwrap();
    assert_eq!(first.digest(), second.digest());
    assert_eq!(machine.applied_index(), 7);
}

#[test]
fn stale_backend_generation_is_rejected_before_apply() {
    let mut machine = fixture_machine();
    let error = machine.apply_committed(11, 1, command_for_generation(9)).unwrap_err();
    assert_eq!(error.code(), "DTG-SHARD-STALE-GENERATION");
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-shard --test determinism

Expected: FAIL with unresolved ShardStateMachine.

- [ ] **Step 3: Implement command and apply boundaries**

~~~rust
pub enum ShardCommand {
    CommitSingleShard(CommitSingleShard),
    PrewriteIntent(PrewriteIntent),
    RecordHomeDecision(RecordHomeDecision),
    FinalizeParticipant(FinalizeParticipant),
    AdvanceClosedTimestamp(AdvanceClosedTimestamp),
    InstallSnapshot(InstallSnapshot),
    Migration(MigrationCommand),
}

pub fn apply_committed(
    &mut self,
    term: u64,
    index: u64,
    command: ShardCommand,
) -> Result<ApplyOutcome, ShardError>;
~~~

Validation produces a deterministic CommittedShardBatch. Only ReplicaStateStore applies business
state. ConsensusStore persists Raft state.

- [ ] **Step 4: Implement Multi-Raft ShardHost lifecycle**

ShardHost owns a map from replica identity to one RaftReplica and supports add, start, stop,
transfer-leader, observe, and sealed-remove operations. It has no node-global backend field.

- [ ] **Step 5: Run focused recovery tests**

Run: cargo test -p dtg-shard

Expected: PASS for replay, WAL restart, snapshot suffix recovery, stale epoch, stale generation,
and independent multi-Raft tests.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-shard
git commit -m "feat: add clean-break Shard and Raft runtime"
~~~

### Task 8: Implement Temporal Snapshot Isolation and distributed transactions

**Files:**
- Modify: crates/execution/dtg-transaction/Cargo.toml
- Create: crates/execution/dtg-transaction/src/snapshot.rs
- Create: crates/execution/dtg-transaction/src/overlay.rs
- Create: crates/execution/dtg-transaction/src/conflict.rs
- Create: crates/execution/dtg-transaction/src/coordinator.rs
- Create: crates/execution/dtg-transaction/src/participant.rs
- Create: crates/execution/dtg-transaction/src/recovery.rs
- Modify: crates/execution/dtg-transaction/src/lib.rs
- Test: crates/execution/dtg-transaction/tests/snapshot_isolation.rs
- Test: crates/execution/dtg-transaction/tests/two_phase_commit.rs

**Interfaces:**
- Consumes: ShardCommand submission, Meta timestamp allocation, dtg-storage reads.
- Produces: SnapshotToken, TransactionContext, TemporalTxnCoordinator, ParticipantService.

- [ ] **Step 1: Write failing interval-conflict tests**

~~~rust
#[test]
fn overlapping_post_snapshot_write_conflicts() {
    let committed = interval_write(10, 30, transaction_time(50));
    let staged = interval_write(20, 40, transaction_time(40));
    assert_eq!(
        detect_conflict(&staged, &[committed]),
        Err(TxnError::WriteConflict)
    );
}

#[test]
fn disjoint_valid_time_corrections_can_commit() {
    let committed = interval_write(10, 20, transaction_time(50));
    let staged = interval_write(20, 30, transaction_time(40));
    assert_eq!(detect_conflict(&staged, &[committed]), Ok(()));
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-transaction --test snapshot_isolation

Expected: FAIL with unresolved transaction types.

- [ ] **Step 3: Implement snapshot tokens and the transaction overlay**

~~~rust
pub struct SnapshotToken {
    pub transaction_id: TransactionId,
    pub start_time: TransactionTime,
    pub catalog_version: Version,
    pub shards: std::collections::BTreeMap<ShardId, ShardSnapshotFence>,
}

pub struct ShardSnapshotFence {
    pub placement_epoch: PlacementEpoch,
    pub backend_generation: BackendGeneration,
    pub applied_index: u64,
    pub closed_time: TransactionTime,
}
~~~

- [ ] **Step 4: Implement one-phase and Home-Shard two-phase commit**

Single-Shard commit submits CommitSingleShard. Multi-Shard commit prewrites every participant,
persists Commit or Abort on the deterministic Home Shard, then idempotently finalizes. Recovery
reads the Home decision; absence remains unresolved and never becomes an implicit commit.

- [ ] **Step 5: Run crash matrix tests**

Run: cargo test -p dtg-transaction

Expected: PASS for conflict, referential integrity, single-Shard fast path, every 2PC crash point,
duplicate delivery, stale epoch, stale generation, and Home recovery.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-transaction
git commit -m "feat: add temporal snapshot isolation transactions"
~~~

### Task 9: Add read barriers and logical replica snapshots

**Files:**
- Create: crates/execution/dtg-shard/src/read.rs
- Create: crates/execution/dtg-shard/src/snapshot.rs
- Modify: crates/execution/dtg-shard/src/lib.rs
- Test: crates/execution/dtg-shard/tests/read_barriers.rs
- Test: crates/execution/dtg-shard/tests/replica_snapshot.rs

**Interfaces:**
- Produces: ReadMode, ReadPermit, FollowerReadProof, ReplicaSnapshotManifest,
  create_replica_snapshot, install_replica_snapshot.

- [ ] **Step 1: Write failing follower-read fence tests**

~~~rust
#[test]
fn historical_read_still_rejects_stale_generation() {
    let proof = proof(12, 4, 8, 100, transaction_time(90));
    let local = local_state(12, 5, 8, 100, transaction_time(90));
    assert_eq!(
        validate_follower_read(&proof, &local, transaction_time(40)),
        Err(ReadError::StaleBackendGeneration)
    );
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-shard --test read_barriers

Expected: FAIL with unresolved read proof types.

- [ ] **Step 3: Implement leader and follower read permits**

Linearizable permits require ReadIndex and local applied wait. Follower permits compare leader
term, placement epoch, backend generation, applied index, closed timestamp, and requested
transaction time.

- [ ] **Step 4: Implement provider-independent replica snapshots**

~~~rust
pub struct ReplicaSnapshotManifest {
    pub format: Version,
    pub binding: ReplicaBinding,
    pub last_included_term: u64,
    pub last_included_index: u64,
    pub logical_digest: Digest32,
    pub chunks: u32,
    pub bytes: u64,
}
~~~

Snapshot installation imports into a candidate namespace, verifies every chunk and final digest,
then atomically activates the complete snapshot.

- [ ] **Step 5: Run tests**

Run: cargo test -p dtg-shard

Expected: PASS for ReadIndex, safe-time, stale fences, snapshot corruption, interrupted install,
restart, and WAL suffix recovery.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-shard
git commit -m "feat: add consistent reads and logical replica snapshots"
~~~

### Task 10: Implement planning and physical fragmentation

**Files:**
- Modify: crates/execution/dtg-plan/Cargo.toml
- Create: crates/execution/dtg-plan/src/catalog.rs
- Create: crates/execution/dtg-plan/src/capability.rs
- Create: crates/execution/dtg-plan/src/logical.rs
- Create: crates/execution/dtg-plan/src/physical.rs
- Create: crates/execution/dtg-plan/src/fragment.rs
- Create: crates/execution/dtg-plan/src/validate.rs
- Modify: crates/execution/dtg-plan/src/lib.rs
- Test: crates/execution/dtg-plan/tests/planning.rs
- Test: crates/execution/dtg-plan/tests/pushdown.rs

**Interfaces:**
- Consumes: LogicalProgram, catalog snapshot, CapabilityManifest.
- Produces: Planner, plan(program, context) -> PhysicalPlan, PlanFragment, StorageAccess,
  PushdownDecision.

- [ ] **Step 1: Write failing capability-pinning tests**

~~~rust
#[test]
fn plan_pins_epoch_generation_and_capability_digest() {
    let plan = plan(point_query(), &fixture_context()).unwrap();
    let fence = plan.fragments()[0].fence();
    assert_eq!(fence.placement_epoch().get(), 7);
    assert_eq!(fence.backend_generation().get(), 3);
    assert_eq!(fence.capability_digest(), fixture_capabilities().digest());
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-plan --test planning --test pushdown

Expected: FAIL because physical planning types do not exist.

- [ ] **Step 3: Implement execution-owned physical types**

~~~rust
pub struct PhysicalPlan {
    pub version: Version,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<Exchange>,
    pub result_schema: RowSchema,
}

pub enum StorageAccess {
    Logical(LogicalReadRequest),
    Pushdown {
        request: PushdownRequest,
        guarantee: PushdownGuarantee,
        residual: Option<PhysicalExpr>,
    },
}

pub struct Planner;

impl Planner {
    pub fn plan(
        &self,
        program: &LogicalProgram,
        context: &PlanningContext,
    ) -> Result<PhysicalPlan, PlanError> {
        plan(program, context)
    }
}
~~~

- [ ] **Step 4: Implement exact, residual, and unsupported planning**

Exact removes the residual only when all temporal, null, duplicate, order, and snapshot guarantees
match. ResidualRequired retains an execution expression. Unsupported chooses a bounded logical
access or returns PlanError::NoBoundedAccess.

- [ ] **Step 5: Run tests and dependency scan**

Run: cargo test -p dtg-plan

Expected: PASS.

Run: cargo tree -p dtg-plan

Expected: DTGProxy dependencies are only dtg-language-ir, dtg-storage, and dtg-kernel.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-plan
git commit -m "feat: add clean-break distributed query planning"
~~~

### Task 11: Implement the bounded columnar query runtime

**Files:**
- Modify: crates/execution/dtg-query/Cargo.toml
- Create: crates/execution/dtg-query/src/batch.rs
- Create: crates/execution/dtg-query/src/budget.rs
- Create: crates/execution/dtg-query/src/operator.rs
- Create: crates/execution/dtg-query/src/expression.rs
- Create: crates/execution/dtg-query/src/storage_source.rs
- Create: crates/execution/dtg-query/src/exchange.rs
- Create: crates/execution/dtg-query/src/runtime.rs
- Modify: crates/execution/dtg-query/src/lib.rs
- Test: crates/execution/dtg-query/tests/runtime.rs
- Test: crates/execution/dtg-query/tests/residual.rs
- Test: crates/execution/dtg-query/tests/backpressure.rs

**Interfaces:**
- Consumes: PhysicalPlan, TemporalReadView, SnapshotToken.
- Produces: ColumnBatch, QueryBudget, QueryRuntime, QueryStream.

- [ ] **Step 1: Write failing residual and budget tests**

~~~rust
#[tokio::test]
async fn residual_filter_runs_after_backend_candidate_filter() {
    let rows = execute(fixture_plan_with_residual(), fixture_store()).await.unwrap();
    assert_eq!(rows.vertex_ids(), vec![vertex_id(2)]);
}

#[tokio::test]
async fn scan_budget_fails_before_unbounded_materialization() {
    let error = execute_with_budget(global_scan(), QueryBudget::rows(10))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DTG-QUERY-ROW-BUDGET");
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-query --test runtime --test residual --test backpressure

Expected: FAIL with unresolved runtime types.

- [ ] **Step 3: Implement column batches and operators**

~~~rust
pub trait Operator: Send {
    fn schema(&self) -> &RowSchema;
    fn next_batch<'a>(&'a mut self, context: &'a mut QueryContext)
        -> QueryFuture<'a, Option<ColumnBatch>>;
}

pub struct QueryBudget {
    pub max_rows: u64,
    pub max_scan_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_network_bytes: u64,
    pub deadline: std::time::Instant,
}
~~~

Implement point/scan source, expand, filter, project, hash join, aggregate, sort, limit, exchange,
overlay, and deterministic merge operators. Every loop charges a budget and checks cancellation.

- [ ] **Step 4: Run query tests**

Run: cargo test -p dtg-query

Expected: PASS for column typing, temporal rows, residual semantics, deterministic merge,
backpressure, cancellation, deadline, memory, and network budgets.

- [ ] **Step 5: Commit**

~~~bash
git add crates/execution/dtg-query
git commit -m "feat: add bounded columnar query execution"
~~~

### Task 12: Define the incompatible internal cluster protocol

**Files:**
- Modify: crates/execution/dtg-cluster-protocol/Cargo.toml
- Create: crates/execution/dtg-cluster-protocol/build.rs
- Create: crates/execution/dtg-cluster-protocol/proto/dtg_cluster_v2.proto
- Create: crates/execution/dtg-cluster-protocol/src/context.rs
- Create: crates/execution/dtg-cluster-protocol/src/validate.rs
- Modify: crates/execution/dtg-cluster-protocol/src/lib.rs
- Test: crates/execution/dtg-cluster-protocol/tests/contracts.rs

**Interfaces:**
- Produces: validated v2 RPC contexts and generated Gateway/Data/Meta/Controller services.

- [ ] **Step 1: Write failing wire-validation tests**

~~~rust
#[test]
fn shard_context_requires_backend_generation() {
    let wire = proto::ShardContext {
        graph_id: 7,
        shard_id: 9,
        placement_epoch: 11,
        backend_generation: 0,
        ..valid_context()
    };
    assert_eq!(
        ShardRequestContext::try_from(wire).unwrap_err().code(),
        "DTG-PROTOCOL-ZERO-GENERATION"
    );
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-cluster-protocol

Expected: FAIL because v2 messages do not exist.

- [ ] **Step 3: Define v2 envelopes**

~~~protobuf
syntax = "proto3";
package dtgproxy.cluster.v2;

message RequestContext {
  uint32 protocol_major = 1;
  uint32 protocol_minor = 2;
  bytes cluster_id = 3;
  bytes request_id = 4;
  uint64 deadline_unix_ms = 5;
  bytes trace_context = 6;
}

message ShardContext {
  RequestContext request = 1;
  uint64 graph_id = 2;
  uint32 shard_id = 3;
  uint64 placement_epoch = 4;
  uint64 backend_generation = 5;
  uint64 catalog_version = 6;
}
~~~

Add bounded fragment, batch, transaction, Raft, replica snapshot, observation, and typed status
messages. Conversion validates versions, identifier lengths, limits, and checksums before
allocation.

- [ ] **Step 4: Run protocol tests**

Run: cargo test -p dtg-cluster-protocol

Expected: PASS.

Run: if rg -n "dtgproxy.cluster.v1|cluster_protocol" crates/execution/dtg-cluster-protocol; then exit 1; fi

Expected: no matches.

- [ ] **Step 5: Commit**

~~~bash
git add crates/execution/dtg-cluster-protocol
git commit -m "feat: define DTGProxy cluster protocol v2"
~~~

### Task 13: Implement the native PostgreSQL provider

**Files:**
- Modify: crates/storage/dtg-storage-postgres/Cargo.toml
- Create: crates/storage/dtg-storage-postgres/migrations/0001_replica_schema.sql
- Create: crates/storage/dtg-storage-postgres/src/config.rs
- Create: crates/storage/dtg-storage-postgres/src/schema.rs
- Create: crates/storage/dtg-storage-postgres/src/apply.rs
- Create: crates/storage/dtg-storage-postgres/src/read_view.rs
- Create: crates/storage/dtg-storage-postgres/src/snapshot.rs
- Modify: crates/storage/dtg-storage-postgres/src/lib.rs
- Test: crates/storage/dtg-storage-postgres/tests/sql_contract.rs
- Test: crates/storage/dtg-storage-postgres/tests/live_tck.rs

**Interfaces:**
- Produces: PostgresReplicaStore::open and the shared storage contract.

- [ ] **Step 1: Write failing SQL shape tests**

~~~rust
#[test]
fn schema_uses_typed_tables_without_canonical_kv() {
    let sql = include_str!("../migrations/0001_replica_schema.sql");
    for table in [
        "replica_owner", "current_vertex", "current_edge", "vertex_history",
        "edge_history", "adjacency", "transaction_state", "replica_meta",
    ] {
        assert!(sql.contains(table));
    }
    assert!(!sql.to_ascii_lowercase().contains("canonical_kv"));
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-storage-postgres --test sql_contract

Expected: FAIL because the native schema does not exist.

- [ ] **Step 3: Implement ownership-fenced native tables and atomic apply**

The schema uses typed IDs, validity bounds, transaction time, properties, direction, and indexes.
Apply starts one SQL transaction, locks replica_owner, compares the complete binding, enforces
monotonic applied_index, writes the typed mutations, updates replica_meta, and commits before
acknowledgement.

- [ ] **Step 4: Implement repeatable read views and logical snapshots**

~~~rust
impl ReplicaStateStore for PostgresReplicaStore {
    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            let transaction = self.connection.repeatable_read_only().await?;
            self.verify_fence(&transaction, &fence).await?;
            Ok(Box::new(PostgresReadView::new(transaction, fence)))
        })
    }
}
~~~

- [ ] **Step 5: Run local and live tests**

Run: cargo test -p dtg-storage-postgres --test sql_contract

Expected: PASS.

Run: DTG_POSTGRES_URL=postgres://dtgproxy:dtgproxy@127.0.0.1:5432/dtgproxy cargo test -p dtg-storage-postgres --test live_tck -- --ignored

Expected: PASS against the disposable PostgreSQL service.

- [ ] **Step 6: Commit**

~~~bash
git add crates/storage/dtg-storage-postgres
git commit -m "feat: add native PostgreSQL storage provider"
~~~

### Task 14: Implement the native Neo4j provider

**Files:**
- Modify: crates/storage/dtg-storage-neo4j/Cargo.toml
- Create: crates/storage/dtg-storage-neo4j/src/config.rs
- Create: crates/storage/dtg-storage-neo4j/src/model.rs
- Create: crates/storage/dtg-storage-neo4j/src/apply.rs
- Create: crates/storage/dtg-storage-neo4j/src/read_view.rs
- Create: crates/storage/dtg-storage-neo4j/src/snapshot.rs
- Modify: crates/storage/dtg-storage-neo4j/src/lib.rs
- Test: crates/storage/dtg-storage-neo4j/tests/model_contract.rs
- Test: crates/storage/dtg-storage-neo4j/tests/live_tck.rs

**Interfaces:**
- Produces: Neo4jReplicaStore::open and the shared storage contract.

- [ ] **Step 1: Write failing native-model tests**

~~~rust
#[test]
fn native_model_uses_nodes_relationships_and_owner_fence() {
    let model = NativeModel::v1();
    assert!(model.labels().contains(&"DtgVertex"));
    assert!(model.labels().contains(&"DtgVersion"));
    assert!(model.relationships().contains(&"DTG_EDGE"));
    assert!(model.constraints().iter().any(|value| value.contains("namespace_id")));
    assert!(!model.labels().contains(&"CanonicalKv"));
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-storage-neo4j --test model_contract

Expected: FAIL because NativeModel does not exist.

- [ ] **Step 3: Implement native graph apply**

Use deterministic node and relationship identifiers plus namespace_id on every entity. One Query
API transaction verifies the owner marker and applied index, mutates Current and History native
entities and relationships, writes transaction metadata, advances applied index, and commits.

- [ ] **Step 4: Implement explicit transaction read views and snapshots**

The read view owns one Neo4j transaction identifier until close. Every generated Cypher predicate
starts with namespace_id and binding generation parameters. Snapshot export emits typed logical
records, not backend Cypher or canonical KV.

- [ ] **Step 5: Run local and live tests**

Run: cargo test -p dtg-storage-neo4j --test model_contract

Expected: PASS.

Run: DTG_NEO4J_URL=http://127.0.0.1:7474 DTG_NEO4J_USER=neo4j DTG_NEO4J_PASSWORD=dtgproxy cargo test -p dtg-storage-neo4j --test live_tck -- --ignored

Expected: PASS against disposable Neo4j 5.26.

- [ ] **Step 6: Commit**

~~~bash
git add crates/storage/dtg-storage-neo4j
git commit -m "feat: add native Neo4j storage provider"
~~~

### Task 15: Add the third-party remote storage protocol and client

**Files:**
- Modify: crates/storage/dtg-storage-remote-protocol/Cargo.toml
- Create: crates/storage/dtg-storage-remote-protocol/build.rs
- Create: crates/storage/dtg-storage-remote-protocol/proto/dtg_storage_v1.proto
- Modify: crates/storage/dtg-storage-remote-protocol/src/lib.rs
- Modify: crates/storage/dtg-storage-remote/Cargo.toml
- Create: crates/storage/dtg-storage-remote/src/client.rs
- Create: crates/storage/dtg-storage-remote/src/read_view.rs
- Create: crates/storage/dtg-storage-remote/src/reference_server.rs
- Modify: crates/storage/dtg-storage-remote/src/lib.rs
- Test: crates/storage/dtg-storage-remote/tests/conformance.rs

**Interfaces:**
- Produces: StorageRemoteClient implementing dtg-storage contracts and ReferenceStorageServer.

- [ ] **Step 1: Write failing handshake tests**

~~~rust
#[tokio::test]
async fn major_contract_mismatch_fails_closed() {
    let server = reference_server_with_contract_major(2).await;
    let error = StorageRemoteClient::connect(server.uri(), fixture_binding())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DTG-REMOTE-CONTRACT-MAJOR");
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-storage-remote

Expected: FAIL because remote protocol and client are absent.

- [ ] **Step 3: Define the versioned remote service**

~~~protobuf
syntax = "proto3";
package dtgproxy.storage.v1;

service Storage {
  rpc Handshake(HandshakeRequest) returns (HandshakeResponse);
  rpc Apply(ApplyRequest) returns (ApplyResponse);
  rpc BeginRead(BeginReadRequest) returns (BeginReadResponse);
  rpc Read(ReadRequest) returns (ReadResponse);
  rpc EndRead(EndReadRequest) returns (EndReadResponse);
  rpc ExportSnapshot(ExportSnapshotRequest) returns (stream SnapshotChunk);
  rpc ImportSnapshot(stream SnapshotChunk) returns (ImportSnapshotResponse);
  rpc Health(HealthRequest) returns (HealthResponse);
}
~~~

Every request includes protocol version, contract version, request ID, deadline, complete binding,
and bounded payload metadata. Apply includes idempotency key, Raft index, and digest.

- [ ] **Step 4: Implement client and reference conformance server**

The client maps typed errors without exposing transport errors above dtg-storage. Read sessions are
explicit and always closed. The reference server uses the dtg-storage test store and intentionally
supports capability subsets for conformance tests.

- [ ] **Step 5: Run protocol tests**

Run: cargo test -p dtg-storage-remote-protocol -p dtg-storage-remote

Expected: PASS for version mismatch, checksums, message limits, idempotency, read-session fencing,
flow control, snapshot resume, and capability subsets.

- [ ] **Step 6: Commit**

~~~bash
git add crates/storage/dtg-storage-remote-protocol crates/storage/dtg-storage-remote
git commit -m "feat: add versioned third-party storage protocol"
~~~

### Task 16: Implement Snapshot CSR and the closed built-in algorithm catalog

**Files:**
- Modify: crates/execution/dtg-analytics/Cargo.toml
- Create: crates/execution/dtg-analytics/src/csr.rs
- Create: crates/execution/dtg-analytics/src/projection.rs
- Create: crates/execution/dtg-analytics/src/catalog.rs
- Create: crates/execution/dtg-analytics/src/traversal.rs
- Create: crates/execution/dtg-analytics/src/shortest_path.rs
- Create: crates/execution/dtg-analytics/src/components.rs
- Create: crates/execution/dtg-analytics/src/centrality.rs
- Create: crates/execution/dtg-analytics/src/community.rs
- Create: crates/execution/dtg-analytics/src/temporal.rs
- Modify: crates/execution/dtg-analytics/src/lib.rs
- Test: crates/execution/dtg-analytics/tests/csr.rs
- Test: crates/execution/dtg-analytics/tests/algorithm_oracles.rs

**Interfaces:**
- Consumes: SnapshotToken and Shard projection parts.
- Produces: SnapshotCsr, ProjectionSpec, AlgorithmCatalog, run_builtin.

- [ ] **Step 1: Write failing stable-order CSR test**

~~~rust
#[test]
fn csr_vertex_order_is_stable_across_part_arrival_order() {
    let left = SnapshotCsr::assemble(vec![part_b(), part_a()], budget()).unwrap();
    let right = SnapshotCsr::assemble(vec![part_a(), part_b()], budget()).unwrap();
    assert_eq!(left.digest(), right.digest());
    assert_eq!(left.vertex_ids(), right.vertex_ids());
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-analytics --test csr

Expected: FAIL because SnapshotCsr is absent.

- [ ] **Step 3: Implement Snapshot CSR and projection budgets**

~~~rust
pub struct SnapshotCsr {
    pub snapshot: SnapshotToken,
    pub vertex_ids: Vec<VertexId>,
    pub offsets: Vec<u64>,
    pub neighbors: Vec<u32>,
    pub edge_ids: Vec<EdgeId>,
    pub reverse: Option<ReverseCsr>,
    pub digest: Digest32,
}
~~~

Assembly sorts stable IDs, validates part snapshot provenance, rejects duplicates and gaps, charges
memory/spill budgets, and computes one content digest.

- [ ] **Step 4: Port algorithms behind closed IDs**

Implement BFS, DFS, SSSP, APSP where bounded, SCC, WCC, PageRank, degree/closeness/betweenness
centrality, triangle count, clustering coefficient, k-core, label propagation, Louvain, earliest
arrival, latest departure, temporal reachability, temporal motif, and change-point algorithms.
Every loop has deterministic ties, cancellation, and declared resource limits.

- [ ] **Step 5: Run oracle and cancellation tests**

Run: cargo test -p dtg-analytics

Expected: PASS against small hand-built graph oracles, partition-order permutations, cancellation,
iteration limits, and memory limits.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-analytics
git commit -m "feat: add Snapshot CSR and built-in graph analytics"
~~~

### Task 17: Implement durable asynchronous T-Cypher analytics

**Files:**
- Create: crates/execution/dtg-analytics/src/job.rs
- Create: crates/execution/dtg-analytics/src/ledger.rs
- Create: crates/execution/dtg-analytics/src/scheduler.rs
- Create: crates/execution/dtg-analytics/src/artifact.rs
- Modify: crates/execution/dtg-analytics/src/lib.rs
- Test: crates/execution/dtg-analytics/tests/jobs.rs
- Test: crates/execution/dtg-analytics/tests/job_recovery.rs

**Interfaces:**
- Produces: AnalyticsJobSpec, AnalyticsJobState, AnalyticsLedger, AnalyticsScheduler,
  AnalyticsArtifactManifest.

- [ ] **Step 1: Write failing lease-fencing tests**

~~~rust
#[test]
fn expired_worker_cannot_publish_result() {
    let mut ledger = fixture_ledger();
    let lease = ledger.claim(job_id(1), worker_id(4), time(10)).unwrap();
    ledger.expire_leases(time(20)).unwrap();
    let error = ledger.publish_result(lease, result_manifest()).unwrap_err();
    assert_eq!(error.code(), "DTG-ANALYTICS-STALE-LEASE");
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-analytics --test jobs --test job_recovery

Expected: FAIL because durable job types do not exist.

- [ ] **Step 3: Implement the replicated job state machine**

~~~rust
pub enum AnalyticsJobState {
    Queued,
    Claimed { worker: WorkerId, lease_epoch: u64, expires_at: u64 },
    Running { worker: WorkerId, lease_epoch: u64, checkpoint: Option<u64> },
    Succeeded { result_generation: u64 },
    Failed { retryable: bool, code: String },
    Cancelled,
    Tombstoned,
}
~~~

All transitions use expected-state and lease-epoch compare-and-set. Snapshot and artifact
generations have durable retention pins and content digests.

- [ ] **Step 4: Implement scheduler recovery**

The scheduler claims only statically compiled BuiltInAlgorithmId jobs, obtains projection parts,
resumes compatible checkpoints, publishes versioned results, and stops promptly on cancellation or
lease loss.

- [ ] **Step 5: Run analytics job tests**

Run: cargo test -p dtg-analytics

Expected: PASS for claim races, restart, retry, cancellation, checkpoint, result publication,
retention GC, and tombstones.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-analytics
git commit -m "feat: add durable asynchronous T-Cypher analytics"
~~~

### Task 18: Implement catalog, bindings, and reconciliation

**Files:**
- Modify: crates/execution/dtg-control/Cargo.toml
- Create: crates/execution/dtg-control/src/catalog.rs
- Create: crates/execution/dtg-control/src/binding.rs
- Create: crates/execution/dtg-control/src/placement.rs
- Create: crates/execution/dtg-control/src/observation.rs
- Create: crates/execution/dtg-control/src/reconcile.rs
- Modify: crates/execution/dtg-control/src/lib.rs
- Test: crates/execution/dtg-control/tests/catalog.rs
- Test: crates/execution/dtg-control/tests/homogeneity.rs

**Interfaces:**
- Produces: CatalogState, CatalogCommand, ShardPlacement, ReplicaBindingRecord,
  ObservedReplicaState, Reconciler.

- [ ] **Step 1: Write failing homogeneity tests**

~~~rust
#[test]
fn one_generation_rejects_mixed_backend_classes() {
    let placement = placement_with_replicas([
        replica_with_class(fjall_class()),
        replica_with_class(postgres_class()),
    ]);
    assert_eq!(
        placement.validate().unwrap_err().code(),
        "DTG-CONTROL-MIXED-BACKEND-CLASS"
    );
}

#[test]
fn one_node_accepts_independent_heterogeneous_shards() {
    let node = observed_node([
        shard_observation(1, fjall_class()),
        shard_observation(2, neo4j_class()),
    ]);
    assert!(node.validate().is_ok());
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-control --test catalog --test homogeneity

Expected: FAIL because catalog types do not exist.

- [ ] **Step 3: Implement authoritative catalog types**

~~~rust
pub struct ShardPlacement {
    pub graph_id: GraphId,
    pub shard_id: ShardId,
    pub placement_epoch: PlacementEpoch,
    pub active_generation: BackendGeneration,
    pub backend_class: BackendClass,
    pub replicas: Vec<ReplicaBindingRecord>,
}
~~~

Catalog commands use expected catalog version and produce immutable lineage. Credentials remain
references, never catalog plaintext.

- [ ] **Step 4: Implement idempotent reconciliation**

Reconciler compares desired catalog state with observations and emits typed actions: allocate,
start learner, promote, transfer leader, seal, migrate, and delete namespace. It never mutates
authority outside Meta commands.

- [ ] **Step 5: Run tests**

Run: cargo test -p dtg-control

Expected: PASS for catalog CAS, stale observations, homogeneous groups, heterogeneous nodes,
lineage, retention pins, and deterministic actions.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-control
git commit -m "feat: add clean-break control-plane catalog"
~~~

### Task 19: Implement generational online backend migration

**Files:**
- Create: crates/execution/dtg-control/src/migration.rs
- Create: crates/execution/dtg-shard/src/migration.rs
- Modify: crates/execution/dtg-control/src/lib.rs
- Modify: crates/execution/dtg-shard/src/command.rs
- Test: crates/execution/dtg-control/tests/migration_state.rs
- Test: crates/execution/dtg-control/tests/migration_crash_matrix.rs
- Test: crates/execution/dtg-control/tests/six_provider_directions.rs

**Interfaces:**
- Produces: MigrationId, MigrationRecord, MigrationState, MigrationAction,
  MigrationCommand, MigrationReceipt.

- [ ] **Step 1: Write failing one-active-generation tests**

~~~rust
#[test]
fn activation_is_monotonic_and_fences_old_epoch() {
    let mut migration = prepared_migration(epoch(7), generation(3), generation(4));
    migration.activate(epoch(8)).unwrap();
    assert_eq!(migration.active_generation(), generation(4));
    assert_eq!(migration.accepts(epoch(7), generation(3)), false);
    assert_eq!(migration.accepts(epoch(8), generation(4)), true);
}

#[test]
fn rollback_uses_a_new_generation() {
    let mut migration = active_in_grace(generation(4));
    let rollback = migration.rollback(epoch(9), generation(5)).unwrap();
    assert_eq!(rollback.generation(), generation(5));
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-control --test migration_state

Expected: FAIL because the new migration state machine is absent.

- [ ] **Step 3: Implement durable phases**

~~~rust
pub enum MigrationState {
    Allocating,
    Backfilling { snapshot_index: u64 },
    Mirroring { snapshot_index: u64 },
    Prepared { verified_index: u64, digest: Digest32 },
    Activating { next_epoch: PlacementEpoch },
    Grace { activated_epoch: PlacementEpoch, activated_at: u64 },
    Completed,
    Aborted,
}
~~~

Every transition records expected source generation, target generation, target backend class, all
replica receipts, and catalog version. All voters must prove equal index and logical digest.

- [ ] **Step 4: Implement snapshot, mirror, cutover, grace, cleanup, and rollback**

Before activation the source is authoritative. Mirroring acknowledges each committed command only
after both stores durably apply. Activation first commits E+1/G+1 in Shard Raft and then publishes
it in Meta. Grace keeps reverse mirroring. Cleanup requires exact owner identity and pins released.
Rollback activates the retained physical namespace as a new generation and epoch.

- [ ] **Step 5: Run crash matrix and six directions**

Run: cargo test -p dtg-control --test migration_crash_matrix

Expected: PASS for crash-before, crash-after, retry, stale controller, digest mismatch, namespace
collision, abort, grace failure, rollback, and cleanup fencing.

Run: cargo test -p dtg-control --test six_provider_directions

Expected: PASS for all six directed Fjall/PostgreSQL/Neo4j pairs using provider TCK fixtures.

- [ ] **Step 6: Commit**

~~~bash
git add crates/execution/dtg-control crates/execution/dtg-shard
git commit -m "feat: add generational online backend migration"
~~~

### Task 20: Assemble the stable execution facade

**Files:**
- Modify: crates/execution/dtg-execution/Cargo.toml
- Create: crates/execution/dtg-execution/src/gateway.rs
- Create: crates/execution/dtg-execution/src/data.rs
- Create: crates/execution/dtg-execution/src/meta.rs
- Create: crates/execution/dtg-execution/src/controller.rs
- Modify: crates/execution/dtg-execution/src/lib.rs
- Test: crates/execution/dtg-execution/tests/facades.rs

**Interfaces:**
- Produces: GatewayExecution, DataExecution, MetaExecution, ControllerExecution, ProviderResolver,
  ProviderResolverSet, and their builders.

- [ ] **Step 1: Write failing composition tests**

~~~rust
#[test]
fn data_builder_accepts_multiple_provider_resolvers() {
    let runtime = DataExecution::builder()
        .with_provider(ProviderKind::Fjall, fjall_resolver())
        .with_provider(ProviderKind::Neo4j, neo4j_resolver())
        .build()
        .unwrap();
    assert_eq!(runtime.provider_kinds().len(), 2);
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-execution

Expected: FAIL because facade builders do not exist.

- [ ] **Step 3: Implement facade builders**

~~~rust
pub struct GatewayExecution {
    language: dtg_language::Language,
    planner: dtg_plan::Planner,
    query: dtg_query::QueryRuntime,
    transactions: dtg_transaction::TemporalTxnCoordinator,
    analytics: dtg_analytics::AnalyticsScheduler,
}

pub struct DataExecution {
    shards: dtg_shard::ShardHost,
    providers: ProviderResolverSet,
}

pub trait ProviderResolver: Send + Sync {
    fn provider_kind(&self) -> ProviderKind;
    fn open<'a>(&'a self, binding: ReplicaBinding)
        -> StoreFuture<'a, std::sync::Arc<dyn ReplicaStateStore>>;
}
~~~

MetaExecution composes catalog, timestamp allocation, leases, and analytics ledger.
ControllerExecution composes observation and reconciliation. Facades expose business use cases,
not internal module types.

- [ ] **Step 4: Run facade and architecture tests**

Run: cargo test -p dtg-execution

Expected: PASS.

Run: bash scripts/check-layered-architecture.sh

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add crates/execution/dtg-execution
git commit -m "feat: expose stable execution composition facades"
~~~

### Task 21: Cut over the Meta and Controller processes

**Files:**
- Modify: crates/processes/dtg-meta/Cargo.toml
- Create: crates/processes/dtg-meta/src/config.rs
- Create: crates/processes/dtg-meta/src/service.rs
- Create: crates/processes/dtg-meta/src/main.rs
- Modify: crates/processes/dtg-controller/Cargo.toml
- Create: crates/processes/dtg-controller/src/config.rs
- Create: crates/processes/dtg-controller/src/service.rs
- Create: crates/processes/dtg-controller/src/main.rs
- Test: crates/processes/dtg-meta/tests/process.rs
- Test: crates/processes/dtg-controller/tests/process.rs

**Interfaces:**
- Consumes: MetaExecution, ControllerExecution, dtg-cluster-protocol v2.
- Produces: dtgproxy-meta and dtgproxy-controller binaries.

- [ ] **Step 1: Write failing thin-process tests**

~~~rust
#[tokio::test]
async fn meta_process_persists_catalog_through_fjall_consensus() {
    let process = fixture_meta_process().await;
    process.propose(create_graph_command()).await.unwrap();
    process.restart().await.unwrap();
    assert!(process.catalog().await.contains_graph(graph_id(1)));
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-meta -p dtg-controller

Expected: FAIL because process services are not implemented.

- [ ] **Step 3: Implement thin composition roots**

main parses config, initializes identity, TLS, telemetry, Fjall consensus namespace, transports,
and execution facade, then serves v2 RPC. No catalog transition or reconciliation rule is defined
inside process crates.

- [ ] **Step 4: Run process and source-boundary tests**

Run: cargo test -p dtg-meta -p dtg-controller

Expected: PASS.

Run: if rg -n "enum (Catalog|Migration)State|struct Reconciler" crates/processes/dtg-meta crates/processes/dtg-controller; then exit 1; fi

Expected: no matches.

- [ ] **Step 5: Commit**

~~~bash
git add crates/processes/dtg-meta crates/processes/dtg-controller
git commit -m "feat: cut Meta and Controller to clean-break runtime"
~~~

### Task 22: Cut over the heterogeneous Data process

**Files:**
- Modify: crates/processes/dtg-data/Cargo.toml
- Create: crates/processes/dtg-data/src/config.rs
- Create: crates/processes/dtg-data/src/provider.rs
- Create: crates/processes/dtg-data/src/service.rs
- Create: crates/processes/dtg-data/src/main.rs
- Test: crates/processes/dtg-data/tests/process.rs
- Test: crates/processes/dtg-data/tests/heterogeneous_shards.rs
- Test: crates/processes/dtg-data/tests/namespace_isolation.rs

**Interfaces:**
- Consumes: DataExecution, all official providers, remote client, v2 protocol.
- Produces: dtgproxy-data binary.

- [ ] **Step 1: Write failing heterogeneous-host test**

~~~rust
#[tokio::test]
async fn one_data_node_hosts_three_independent_backend_classes() {
    let node = fixture_data_node()
        .assign(fjall_shard(1))
        .assign(postgres_shard(2))
        .assign(neo4j_shard(3))
        .start()
        .await
        .unwrap();
    assert_eq!(node.observed_replicas().await.len(), 3);
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-data --test heterogeneous_shards

Expected: FAIL because Data composition is absent.

- [ ] **Step 3: Implement provider resolution per replica binding**

~~~rust
pub struct FjallResolver {
    root: std::path::PathBuf,
}

impl dtg_execution::ProviderResolver for FjallResolver {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Fjall
    }

    fn open<'a>(&'a self, binding: ReplicaBinding)
        -> StoreFuture<'a, std::sync::Arc<dyn ReplicaStateStore>>
    {
        Box::pin(async move {
            let path = self.root.join(binding.namespace_id().to_string());
            let store = dtg_storage_fjall::FjallReplicaStore::open(path, binding)?;
            Ok(std::sync::Arc::new(store))
        })
    }
}
~~~

The process resolves credentials and endpoints, constructs providers, and hands them to
DataExecution. It has no node-global backend enum. Official resolvers instantiate providers
directly; only ProviderKind::Remote creates StorageRemoteClient.

- [ ] **Step 4: Run Data tests**

Run: cargo test -p dtg-data

Expected: PASS for mixed Shards, same-group homogeneity rejection, restart, independent failure,
namespace collision, official direct paths, and remote third-party path.

Run: if cargo tree -p dtg-data | rg "adapter-sidecar|adapter-rocksdb"; then exit 1; fi

Expected: no matches.

- [ ] **Step 5: Commit**

~~~bash
git add crates/processes/dtg-data
git commit -m "feat: cut Data to heterogeneous clean-break ShardHost"
~~~

### Task 23: Cut over the Gateway process and Bolt surface

**Files:**
- Modify: crates/processes/dtg-gateway/Cargo.toml
- Create: crates/processes/dtg-gateway/src/config.rs
- Create: crates/processes/dtg-gateway/src/bolt.rs
- Create: crates/processes/dtg-gateway/src/service.rs
- Create: crates/processes/dtg-gateway/src/main.rs
- Test: crates/processes/dtg-gateway/tests/tcypher.rs
- Test: crates/processes/dtg-gateway/tests/transactions.rs
- Test: crates/processes/dtg-gateway/tests/analytics.rs

**Interfaces:**
- Consumes: GatewayExecution and cluster protocol v2 clients.
- Produces: dtgproxy-gateway binary and Bolt/T-Cypher surface.

- [ ] **Step 1: Write failing end-to-end query test**

~~~rust
#[tokio::test]
async fn bolt_query_uses_new_language_and_execution_path() {
    let cluster = fixture_clean_break_cluster().await;
    let rows = cluster
        .bolt()
        .query("MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n.id ORDER BY n.id")
        .param("t", 40_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(rows.ids(), vec![1, 2]);
}
~~~

- [ ] **Step 2: Verify red**

Run: cargo test -p dtg-gateway --test tcypher

Expected: FAIL because the Gateway process is absent.

- [ ] **Step 3: Implement thin Bolt adaptation**

Bolt decoding creates request context and parameters, calls dtg-language compile, then invokes
GatewayExecution for query, transaction, or asynchronous analytics. Bolt encoding consumes typed
rows and errors. No planner, query operator, transaction coordinator, job state machine, or
procedure registry is defined in this crate.

- [ ] **Step 4: Run Gateway tests**

Run: cargo test -p dtg-gateway

Expected: PASS for current, AS OF, CHANGES, write, single/multi-Shard transaction, built-in
analytics submit/status/result/cancel, cancellation, deadline, and error mapping.

Run: if rg -n "ProcedureRegistry|ProcedureProvider|temporal_ir|query_executor" crates/processes/dtg-gateway; then exit 1; fi

Expected: no matches.

- [ ] **Step 5: Commit**

~~~bash
git add crates/processes/dtg-gateway
git commit -m "feat: cut Gateway to normalized T-Cypher execution"
~~~

### Task 24: Certify the four-process clean-break cluster

**Files:**
- Create: config/examples/clean-break-cluster/meta-1.json
- Create: config/examples/clean-break-cluster/controller-1.json
- Create: config/examples/clean-break-cluster/gateway-1.json
- Create: config/examples/clean-break-cluster/data-1.json
- Create: config/examples/clean-break-cluster/data-2.json
- Create: scripts/certify-clean-break.sh
- Create: scripts/tests/certify-clean-break-contract.sh
- Create: crates/processes/dtg-gateway/tests/four_process_cluster.rs
- Test: scripts/tests/certify-clean-break-contract.sh

**Interfaces:**
- Consumes: all new crates and binaries.
- Produces: one repeatable certification command and evidence directory.

- [ ] **Step 1: Write the failing certification contract**

~~~bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/certify-clean-break.sh
scripts/certify-clean-break.sh --contract-only
~~~

- [ ] **Step 2: Verify red**

Run: bash scripts/tests/certify-clean-break-contract.sh

Expected: FAIL because the certification script and fixtures do not exist.

- [ ] **Step 3: Implement certification**

The script runs format, architecture checks, all new workspace tests, live-backend prerequisites,
four processes, RF=3 groups, mixed Data-node Shards, leader failover, follower reads, cross-Shard
transactions, Snapshot CSR algorithms, async analytics, replica snapshot recovery, and all six
backend migrations. It writes versioned JSON evidence with command, exit status, environment
fingerprint, and content digest.

- [ ] **Step 4: Run contract and local certification**

Run: bash scripts/tests/certify-clean-break-contract.sh

Expected: PASS.

Run: scripts/certify-clean-break.sh --local

Expected: PASS with zero failed gates and an evidence manifest path.

- [ ] **Step 5: Commit**

~~~bash
git add config/examples/clean-break-cluster scripts/certify-clean-break.sh scripts/tests/certify-clean-break-contract.sh crates/processes/dtg-gateway/tests/four_process_cluster.rs
git commit -m "test: certify clean-break four-process architecture"
~~~

### Task 25: Delete the legacy architecture and make the new tree exclusive

**Files:**
- Delete: crates/adapter-memory
- Delete: crates/adapter-neo4j
- Delete: crates/adapter-postgres
- Delete: crates/adapter-registry
- Delete: crates/adapter-rocksdb
- Delete: crates/adapter-sidecar
- Delete: crates/storage-api
- Delete: crates/temporal-ir
- Delete: crates/physical-plan
- Delete: crates/query-executor
- Delete: crates/query-optimizer
- Delete: crates/distributed-query
- Delete: crates/procedure-runtime
- Delete: crates/cypher-engine
- Delete: crates/temporal-storage
- Delete: crates/raft-logstore
- Delete: crates/cluster-protocol
- Delete: crates/data-node
- Delete: crates/gateway-node
- Delete: crates/meta-node
- Delete: crates/controller
- Delete: crates/dtgproxy
- Delete or replace: every old-only transitive crate identified by cargo metadata.
- Modify: Cargo.toml
- Modify: Cargo.lock
- Create: scripts/check-clean-break-removal.sh
- Create: scripts/tests/check-clean-break-removal-contract.sh
- Replace: README.md, deployment docs, examples, workflows, benchmarks, and scripts that name or
  invoke the old runtime.

**Interfaces:**
- Consumes: a fully certified new runtime.
- Produces: a workspace in which only the approved architecture is buildable.

- [ ] **Step 1: Write the failing removal gate**

~~~bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

for path in crates/adapter-rocksdb crates/adapter-sidecar crates/adapter-registry crates/storage-api crates/temporal-ir crates/physical-plan crates/query-executor crates/query-optimizer crates/distributed-query crates/procedure-runtime crates/cypher-engine crates/temporal-storage crates/raft-logstore crates/data-node crates/gateway-node crates/meta-node crates/controller crates/dtgproxy
do
  test ! -e "$path"
done

! cargo metadata --format-version 1 | rg -i "rocksdb|adapter-sidecar|procedure-runtime"
! rg -n --glob 'Cargo.toml' --glob '*.rs' --glob '*.proto' "StorageAdapter|TemporalBackendMapping|ProcedureProvider|dtgproxy.cluster.v1"
~~~

- [ ] **Step 2: Verify red before deletion**

Run: bash scripts/tests/check-clean-break-removal-contract.sh

Expected: FAIL and list legacy paths and dependencies.

- [ ] **Step 3: Delete the legacy dependency closure**

Use explicit paths from the failing report. Remove old workspace members and dependencies. Run
cargo metadata after each deletion batch and delete crates that have no new-tree consumer. Keep
only kernel/language/execution/storage/process packages plus provider-independent support that has a
documented allowed dependency edge.

- [ ] **Step 4: Rewrite normative surfaces**

README and current docs describe Fjall/PostgreSQL/Neo4j native providers, third-party remote
storage, heterogeneous Shards, v2 protocols, TSI, Snapshot CSR, analytics, and migration. Remove
old Sidecar commands, RocksDB setup, Adapter SPI docs, old process examples, old benchmark cells,
and workflows that certify the removed architecture. Historical approved specs may remain.

- [ ] **Step 5: Regenerate lockfile and run removal gate**

Run: cargo generate-lockfile

Expected: PASS.

Run: bash scripts/tests/check-clean-break-removal-contract.sh

Expected: PASS.

Run: cargo tree --workspace | rg -i "rocksdb|adapter-sidecar|procedure-runtime"

Expected: no matches.

- [ ] **Step 6: Commit**

~~~bash
git add -A Cargo.toml Cargo.lock crates README.md docs config examples scripts .github
git commit -m "refactor: remove legacy DTGProxy architecture"
~~~

### Task 26: Run the final requirement-by-requirement audit

**Files:**
- Create: docs/verification/dtgproxy-clean-break-architecture.md
- Modify: scripts/certify-clean-break.sh
- Test: entire workspace and live certification matrix.

**Interfaces:**
- Consumes: approved design acceptance criteria and all certification evidence.
- Produces: one final audit mapping every requirement to authoritative evidence.

- [ ] **Step 1: Create the audit matrix with explicit evidence slots**

~~~markdown
| Requirement | Evidence command/artifact | Result |
|---|---|---|
| Minimal kernel and allowed DAG | scripts/check-layered-architecture.sh | |
| Language stops at normalized IR | dtg-language and IR contract tests | |
| Execution capability ownership | package tests and source boundary scans | |
| Native official providers | live TCK and physical-layout tests | |
| Versioned third-party protocol | remote conformance suite | |
| Binding and namespace invariants | distributed isolation tests | |
| Six migration directions | migration certification manifest | |
| Legacy runtime absent | scripts/check-clean-break-removal.sh | |
~~~

- [ ] **Step 2: Run fresh complete verification**

Run: cargo fmt --all --check

Expected: PASS.

Run: cargo clippy --workspace --all-targets --all-features -- -D warnings

Expected: PASS.

Run: cargo test --workspace --all-features

Expected: PASS with zero failed tests.

Run: bash scripts/check-layered-architecture.sh

Expected: PASS.

Run: bash scripts/check-clean-break-removal.sh

Expected: PASS.

Run: scripts/certify-clean-break.sh --live

Expected: PASS with Fjall, PostgreSQL, Neo4j, remote protocol, four processes, failover,
transactions, algorithms, analytics, snapshots, and six migrations all certified.

- [ ] **Step 3: Populate the audit from command outputs**

Record exact command, timestamp, git commit, environment fingerprint, test totals, manifest paths,
and digests. Mark a requirement PASS only when its evidence has the same scope as the requirement.

- [ ] **Step 4: Re-run document and repository scans**

Run: pattern='\b(T''BD|T''ODO|F''IXME|X''XX)\b'; if rg -n "$pattern" docs/verification/dtgproxy-clean-break-architecture.md; then exit 1; fi

Expected: no matches.

Run: git status --short

Expected: only intentionally uncommitted user-owned files, or clean if all prior state was
reconciled.

- [ ] **Step 5: Commit**

~~~bash
git add docs/verification/dtgproxy-clean-break-architecture.md scripts/certify-clean-break.sh
git commit -m "docs: certify DTGProxy clean-break architecture"
~~~

## Execution order and review gates

Tasks are strictly ordered. Review and verify after every task. Tasks 13, 14, and 15 may be
implemented in parallel only after Task 12 and the Task 5 storage contract are stable. Process
cutover tasks start only after Task 20. Legacy deletion starts only after Task 24 passes. The final
goal may be marked complete only after Task 26 proves every acceptance criterion from the approved
design.
