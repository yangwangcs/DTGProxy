# DTGProxy 静态分片多节点查询 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在一次只绑定一种后端的 DTGProxy 集群中，实现静态 owner 路由、Leader 围栏快照读和并行多节点 fragment 查询，并保持 Fjall、PostgreSQL、Kuzu 的独立 adapter 验收。

**Architecture:** `dtg-plan` 从规范化逻辑 read 中提取可确定 owner 的 vertex/adjacency/traversal 访问，并只为其 owner shard 生成 fragment；扫描保留全 shard fan-out。Gateway transport 按 shard 分组并有界并行执行，执行层继续使用已有 `ReadFence`、catalog/version/capability digest 和 immutable read view 作为一致性边界。Data 进程只加载配置选择的一个 provider，三 adapter 通过相同 storage TCK 与单后端进程测试验证。

**Tech Stack:** Rust 2024 workspace、Tokio、Tonic/gRPC、Raft、Fjall、PostgreSQL、Kuzu。

## Global Constraints

- 一次集群运行时只能选择 `fjall`、`postgresql`、`kuzu` 之一；禁止同一 catalog 的 active shard 混用 provider。
- 语言层不得引用 routing、RPC、Raft 或 provider 类型。
- 每个 fragment 必须保留 catalog version、schema version、placement epoch、backend generation、capability digest、applied index 与 immutable snapshot 时间。
- 第一阶段只读 leader；任何分片返回围栏失效或可安全重试错误时，Gateway 不混合结果。
- 不实现迁移、重平衡、split/merge 或 follower read。

---

### Task 1: 单后端 catalog 与静态 owner 路由

**Files:**
- Modify: `crates/execution/dtg-plan/src/catalog.rs`
- Modify: `crates/execution/dtg-plan/src/lib.rs`
- Modify: `crates/execution/dtg-plan/tests/planning.rs`

**Interfaces:**
- Produces `StaticShardRouter::new(&CatalogSnapshot) -> Result<StaticShardRouter, PlanError>`.
- Produces `StaticShardRouter::shard_for_vertex(VertexId) -> ShardId`.
- `CatalogSnapshot::new` rejects active shards with more than one `ProviderKind`.

- [ ] **Step 1: Write failing planner tests**

Add a three-shard Fjall catalog and assert that vertex point lookup and point-anchored adjacency
produce exactly one fragment whose shard is `vertex_id % shard_count` in catalog shard order.
Add a catalog containing Fjall and Kuzu active shards and assert construction returns
`PlanError::InvalidCatalog`.

- [ ] **Step 2: Run the focused test**

Run: `cargo test --locked -p dtg-plan --test planning static_owner`

Expected: FAIL because the current planner broadcasts every access and catalog accepts mixed providers.

- [ ] **Step 3: Implement the smallest routing surface**

Add a private catalog router that sorts active shard IDs and maps `vertex_id.get() % shard_count`
to that sorted list. Store the one validated provider kind in `CatalogSnapshot`; expose it only as
a read-only invariant. Make `plan()` select one shard for `VertexPoint`, `Adjacency`, and
`Traversal`; make scans and edge scans retain fan-out.

- [ ] **Step 4: Run focused and package tests**

Run: `cargo test --locked -p dtg-plan --test planning`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/execution/dtg-plan && git commit -m "feat(plan): route point reads to static shard owners"`

### Task 2: Leader snapshot identity validation

**Files:**
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Modify: `crates/execution/dtg-execution/tests/gateway_process.rs`

**Interfaces:**
- `ShardRoutedGatewayTransport` runs one request per shard concurrently.
- Every returned fragment ID must belong to the request’s routed shard and be present exactly once.
- A routed request whose fragment response is malformed returns safe retry error `DTG-CLUSTER-ROUTING`.

- [ ] **Step 1: Write failing transport tests**

Add two delayed test transports. Assert the second shard may finish first while total completion
time is bounded by the slower request rather than their sum. Add a transport returning a duplicate
fragment ID and assert no materialized result is returned.

- [ ] **Step 2: Run the focused test**

Run: `cargo test --locked -p dtg-execution --test gateway_process shard_routed`

Expected: FAIL because the current implementation awaits each shard request serially.

- [ ] **Step 3: Implement bounded parallel fan-out**

Build the existing per-shard requests before awaiting. Use `futures_util::future::try_join_all`
for the bounded number of catalog shards, then merge only after every request succeeded. Retain
the current request fence in every cloned plan and reject duplicate/missing fragment identities.

- [ ] **Step 4: Run focused and package tests**

Run: `cargo test --locked -p dtg-execution --test gateway_process`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/execution/dtg-execution && git commit -m "feat(gateway): execute shard fragments concurrently"`

### Task 3: 单后端 Data 装配与三 adapter 契约

**Files:**
- Modify: `crates/processes/dtg-data/tests/process.rs`
- Modify: `crates/processes/dtg-data/tests/heterogeneous_shards.rs`
- Modify: `crates/processes/dtg-data/src/service.rs` only if tests expose mixed-resolver composition
- Modify: `README.md`

**Interfaces:**
- `DataNodeBuilder::from_config` exposes exactly the configured provider kind.
- Fjall, PostgreSQL, Kuzu use independent `DataProcessConfig` instances and never load another
  provider as an active replica.

- [ ] **Step 1: Write failing three-provider configuration test**

Construct three independent configs, one for each provider, each with only matching bindings.
Assert the produced Data nodes report exactly the selected provider. Assert the legacy
heterogeneous builder cannot be used by the production config path.

- [ ] **Step 2: Run focused tests**

Run: `cargo test --locked -p dtg-data --test process --test heterogeneous_shards`

Expected: FAIL if a production path can compose mismatching assignment/provider pairs.

- [ ] **Step 3: Implement only required rejection/documentation changes**

Keep test-only heterogeneous fixtures isolated. Reject provider mismatch before opening any
namespace, retain PostgreSQL credential validation only for PostgreSQL, and document the
cluster-wide single-provider invariant.

- [ ] **Step 4: Run Data and storage contract tests**

Run: `cargo test --locked -p dtg-data --test process --test assignments && cargo test --locked -p dtg-storage-fjall --test storage_tck && cargo test --locked -p dtg-storage-kuzu --test storage_tck`

Expected: PASS; PostgreSQL live TCK remains an explicitly configured live verification.

- [ ] **Step 5: Commit**

Run: `git add crates/processes/dtg-data README.md && git commit -m "test(data): certify single-provider node composition"`

### Task 4: 多节点 snapshot 查询验收

**Files:**
- Modify: `crates/processes/dtg-gateway/tests/four_process_cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs` only to add selected-backend topology coverage
- Modify: `docs/audit/performance/2026-08-04-static-shard-multinode.md`

**Interfaces:**
- Three Data processes receive distinct static shard endpoint assignments for one provider.
- Point and adjacency reads address exactly one owner endpoint; scan/count address all endpoints.
- All fragment responses share the fixed `ReadFence` identity.

- [ ] **Step 1: Write failing four-process routing test**

Create three test Data endpoints with disjoint shard assignments. Execute a point lookup, an
adjacency query and a count query through Gateway. Assert endpoint call counts are `1`, `1`, and
`3` respectively; assert all output rows have the same transaction time and catalog revision.

- [ ] **Step 2: Run focused test**

Run: `cargo test --locked -p dtg-gateway --test four_process_cluster static_shard`

Expected: FAIL before routing and parallel transport changes are complete.

- [ ] **Step 3: Implement only test wiring required by existing Gateway/Data protocol**

Use existing `ShardRoutedGatewayTransport`, catalog bindings and fragment `ReadFence`; do not add
a second routing protocol or bypass Data immutable read views.

- [ ] **Step 4: Run each selected-backend diagnostic**

Run the existing ignored selected-backend diagnostic separately with `fjall`, `postgresql`, then
`kuzu`, each using release process binaries and its disposable provider setup. Record environment,
revision, workload, result digest and failures; do not report unavailable live services as passing.

- [ ] **Step 5: Commit**

Run: `git add crates/processes/dtg-gateway docs/audit/performance && git commit -m "test(gateway): certify static-shard snapshot routing"`

### Task 5: Final verification

**Files:**
- Modify: `README.md` only if actual verified behavior differs from its current claims.

- [ ] **Step 1: Run code quality gates**

Run: `cargo fmt --all -- --check && git diff --check && bash scripts/check-layered-architecture.sh && bash scripts/tests/local-cluster-contract.sh`

- [ ] **Step 2: Run targeted workspace tests**

Run: `cargo test --locked -p dtg-plan --test planning && cargo test --locked -p dtg-execution --test gateway_process && cargo test --locked -p dtg-data --test process && cargo test --locked -p dtg-gateway --test four_process_cluster`

- [ ] **Step 3: Commit final documentation corrections**

Run: `git add README.md docs && git commit -m "docs: record static-shard multinode verification"`
