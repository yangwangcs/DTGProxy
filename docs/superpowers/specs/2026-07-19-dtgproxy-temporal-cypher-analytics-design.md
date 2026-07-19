# DTGProxy Temporal Cypher 与时态图分析设计规范

日期：2026-07-19

状态：设计已批准

适用范围：DTGProxy Cypher/Bolt 前端、Temporal IR v2、分布式查询运行时、时态图分析运行时

## 1. 目标

DTGProxy 对外提供完整的 Cypher 查询、写入、过程调用和 Bolt Driver 兼容能力，并在不暴露底层存储差异的前提下，为 RocksDB、Neo4j 和 PostgreSQL 三个认证后端提供统一的双时态图语义；Memgraph 通过与 Neo4j 分离的可选 Adapter 后续进入兼容认证。系统同时支持 PrimaryReplica 和 Shared-Nothing 部署，并提供可插拔的普通图、时态图、增量图和分布式图分析算法。

“完整兼容”在本规范中表示：

- 支持 Cypher 5 冻结兼容档和版本化的 Cypher 25 兼容档。
- 支持 Cypher 读写、子查询、函数、过程、事务及公开数据类型语义。
- 支持 Bolt 握手、PackStream、认证、路由、结果流和事务状态机。
- 标准 Cypher 在未使用 DTG 扩展时不改变语义。
- 支持 DTG 双时态查询、回溯修正和分布式时态事务。
- 相同 Logical IR 在所有受支持后端产生相同的规范结果。
- PrimaryReplica 和 Shared-Nothing 对客户端呈现相同事务及查询行为。
- 算法通过稳定 Provider SPI 热插拔，不绑定单一存储或计算引擎。

明确排除：

- Neo4j 管理、集群运维、用户管理命令的兼容。
- Neo4j Java Plugin 二进制 ABI 兼容。
- Neo4j 专有 GDS API 兼容。
- 允许应用绕过 DTGProxy 直接修改受管理后端。
- 将某个后端方言或执行器作为 DTGProxy 的事实语义来源。

## 2. 已批准技术路线

采用“兼容前端复用、核心自主实现”的混合路线：

```text
openCypher Grammar/TCK ─┐
Grafeo Parser/AST 候选 ─┼─> DTG Binder/Type/Temporal Semantics
Bolt Specification ─────┘                    │
                                             v
                                      Temporal IR v2
                                             │
                          ┌──────────────────┴─────────────────┐
                          v                                    v
                 Distributed Query Runtime              Analytics Runtime
                          │                                    │
                 Backend Adapter SPI                   AnalyticsProvider SPI
                          │                                    │
             RocksDB/Neo4j/PostgreSQL              petgraph/TGLib/LAGraph/
             optional Memgraph                     GraphScope/cuGraph
```

复用边界：

- 文法、TCK、基础 CST/AST 和算法实现尽量复用。
- Binder、类型系统、双时态语义、Temporal IR、分布式计划和事务必须由 DTGProxy 控制。
- 后端不接收用户原始 Cypher，只接收经过能力验证的物理 Fragment。
- 算法引擎不解释 DTG 双时态存储，只消费 DTGProxy 生成的规范图投影。

该路线吸收 GraphScope GIE 的统一 IR 与 M+N 解耦思想、NebulaGraph 的 Parser/Planner/Optimizer/Executor 分层和计划复用思想、TigerGraph 的计算跟随分区数据思想，以及 Galaxybase 的接口/计算/分布式执行/存储分层思想。

## 3. Cypher 兼容档

### 3.1 版本选择

支持：

```cypher
CYPHER 5
MATCH (n) RETURN n
```

```cypher
CYPHER 25
MATCH (n) RETURN n
```

规则：

- 默认语言为 Cypher 25。
- Cypher 5 语义固定为其冻结版本，不随 DTGProxy 升级改变。
- Cypher 25 使用日期基线，例如 `Cypher 25 / 2026.07`。
- 每个编译计划记录语言档、语义基线和查询指纹。
- 新增 Cypher 25 能力先进入 Experimental，再依次进入 Preview 和 Stable。
- Plan Cache 不允许跨语言档复用。

### 3.2 兼容范围

完整数据查询和更新范围包括：

- `MATCH`、`OPTIONAL MATCH`、`WHERE`、`FILTER`。
- `WITH`、`LET`、`RETURN`、`FINISH`。
- `UNWIND`、`FOR`、`UNION`、`UNION ALL`、`NEXT`、`WHEN`。
- `ORDER BY`、`SKIP/OFFSET`、`LIMIT`、`DISTINCT`。
- 聚合、列表、Map、Pattern/List Comprehension。
- `EXISTS`、`COUNT` 和 `CALL {}` 子查询。
- 定长、变长、最短路径和 Cypher 25 路径模式。
- `CREATE`、`MERGE`、`SET`、`REMOVE`、`DELETE`、`DETACH DELETE`、`FOREACH`。
- `CALL ... YIELD`、标量函数、聚合函数和命名过程。
- `CALL {} IN TRANSACTIONS`。
- Schema、索引和约束中与数据图有关的能力。
- Boolean、Integer、Float、String、Bytes、List、Map、Node、Relationship、Path、Temporal、Spatial 和 Vector 类型。
- Cypher 三值逻辑、NULL、比较、排序、溢出和错误语义。

## 4. DTG Temporal Cypher

### 4.1 查询作用域

时态上下文绑定到 `USE` 所选图或查询部分：

```cypher
CYPHER 25
USE accounts
AT VALID_TIME AS OF $valid_time
AT TRANSACTION_TIME AS OF $transaction_time
MATCH (a:Account)-[e:TRANSFER]->(b:Account)
RETURN a, e, b
```

区间查询：

```cypher
USE accounts
AT VALID_TIME FROM $from TO $to
AT TRANSACTION_TIME AS OF $tx
MATCH (a)-[e:OWNS]->(b)
RETURN a, e, b
```

`AT` 被选为 DTG 扩展关键字，避免与 Cypher 25 已采用的 `FOR` Clause 冲突。

### 4.2 DIFF

```cypher
DIFF GRAPH accounts
  AT VALID_TIME AS OF $t1
  AND AS OF $t2
  AT TRANSACTION_TIME AS OF $tx
YIELD element, changeType, before, after
```

`DIFF` 是规范算子而非后端方言。其结果必须包含稳定元素标识、变化种类、前后值和比较上下文。

### 4.3 时间所有权

- 用户可指定 valid time。
- 用户可读取历史 transaction time。
- 新写入的 transaction time 只能由线性一致 TSO 在 Commit 时分配。
- 回溯修正改变 valid-time 事实，但不覆盖已经发生的 transaction-time 历史。
- transaction-time `AS OF` 先冻结系统认知，再执行 valid-time 查询。

## 5. Cypher 与 Bolt 前端

### 5.1 编译管线

```text
Query Text
  -> Version/Option Scanner
  -> Lexer
  -> Concrete Syntax Tree
  -> Versioned Cypher AST
  -> Name Binder
  -> Type Checker
  -> Temporal Scope Resolver
  -> Normalized AST
  -> Temporal Logical IR
```

模块：

- `cypher-syntax`：Lexer、Parser、CST、源码位置和语法诊断。
- `cypher-ast`：Cypher 5、Cypher 25 和 DTG 扩展 AST。
- `cypher-sema`：作用域、类型、聚合、路径和更新语义。
- `cypher-compiler`：规范化、逻辑计划生成和诊断映射。
- `procedure-runtime`：函数、聚合函数和过程目录。

Parser 可在逐文件审计后抽取 Grafeo 的 Apache-2.0 实现。Binder、时态语义和 IR 转换不得依赖完整 Grafeo 数据库内核。

### 5.2 Bolt Server

实现：

- TCP、TLS 和 WebSocket Transport。
- Bolt Manifest 和版本协商。
- PackStream 类型及扩展类型编解码。
- `HELLO`、`LOGON`、`LOGOFF`、`GOODBYE`。
- `RUN`、`PULL`、`DISCARD`、`RESET`、`INTERRUPT`。
- `BEGIN`、`COMMIT`、`ROLLBACK`。
- Auto-Commit 和 Explicit Transaction。
- Bookmark、Timeout、Metadata 和 Access Mode。
- `ROUTE` 和 Driver Routing Table。
- 流式结果、分页、背压、取消和 Summary。

`ROUTE` 只返回 DTGProxy Gateway 地址，不泄漏 Shard、Replica 或后端地址。

### 5.3 Procedure Runtime

提供：

- 内置 Rust Procedure。
- 稳定 C ABI。
- 沙箱化 WASM Procedure。
- Remote Procedure/AnalyticsProvider。

Procedure 描述符必须声明输入输出 Schema、权限、确定性、副作用、资源上限和允许的 Cypher 兼容档。常用 APOC 能力可按许可证选择性重实现，但不加载 APOC JAR。

## 6. Temporal IR v2

### 6.1 分层

```text
Normalized Cypher AST
        -> Temporal Logical IR
        -> Optimized Logical IR
        -> Distributed Physical IR
        -> Backend Fragment / Native Operator / AnalyticsProvider
```

逻辑计划和物理计划使用不同类型，禁止在单个枚举中混合语义算子、分布算子和后端指令。

### 6.2 Logical Plan Header

```rust
TemporalLogicalPlan {
    ir_version,
    query_fingerprint,
    graph_id,
    tenant_id,
    language_profile,
    schema_snapshot,
    temporal_context,
    transaction_context,
    consistency,
    resource_budget,
    root_operator,
}
```

计划固定语言档、Schema、时间上下文、事务快照、Topology Epoch、参数类型、权限指纹、NULL 行为和路径模式。

### 6.3 时态行模型

```text
TemporalRow {
    visible_values,
    valid_region,
    transaction_region,
    provenance,
}
```

语义：

- `AS OF` 将时态域压缩为一个普通 Cypher 快照。
- 区间使用半开区间 `[from, to)`。
- Pattern Join 通过时态区域求交。
- 交集为空的绑定被移除。
- 相邻且可见值相同的区间自动 Coalesce。
- 隐藏时间域默认不返回，可通过时态函数显式读取。
- 区间聚合产生分段常量结果。
- 双区间查询内部使用规范化的二维非重叠矩形集合。

### 6.4 逻辑算子

数据源：

- `VertexScan`、`EdgeScan`、`ElementIdSeek`。
- `PropertyIndexSeek`、`TemporalIndexSeek`。
- `ChangeScan`、`CurrentProjectionScan`、`HistoryScan`。

图模式：

- `Expand`、`ExpandInto`、`VarLengthExpand`。
- `TemporalPathExpand`、`ShortestPath`、`TemporalShortestPath`。
- `PatternJoin`、`PathUniqueness`、`NodeHashJoin`。

关系算子：

- `Filter`、`Project`、`Unwind`。
- `HashJoin`、`MergeJoin`、`OptionalJoin`、`SemiJoin`、`AntiJoin`。
- `Aggregate`、`Distinct`、`Sort`、`TopN`、`Union`。
- `Conditional`、`Sequential`。

时态算子：

- `Snapshot`、`TemporalSlice`、`TemporalJoin`。
- `IntervalIntersect`、`CoalesceIntervals`。
- `HistoryReconstruct`、`VersionResolve`、`BitemporalNormalize`。
- `Diff`、`ChangePoint`、`TemporalAggregate`。

写算子：

- `CreateNode`、`CreateEdge`、`MergePattern`。
- `SetProperty`、`RemoveProperty`、`SetLabel`。
- `Delete`、`DetachDelete`、`TemporalCorrect`、`SchemaMutation`。

扩展算子：

- `ProcedureCall`、`FunctionCall`、`GraphProject`、`AlgorithmCall`。

每个算子声明输入输出 Schema、时态域、分区、顺序、唯一性、副作用、确定性、能力、估计基数和资源预算。

## 7. Optimizer

### 7.1 时态规范化

- 补全隐式 valid/transaction time。
- 将历史属性读取转成 `VersionResolve`。
- 将区间 Pattern Match 转成 `TemporalJoin`。
- 插入必要的 Coalesce。
- 选择 Current Projection 或 History Reconstruction。
- 区分普通更新和回溯修正。

### 7.2 RBO

- Filter、Projection、Limit、TopN 下推。
- 时间谓词优先下推。
- Scan/Expand 融合和 Expand 方向反转。
- 无用元素及属性消除。
- Temporal Slice 下推。
- 相邻 Coalesce 合并。
- 子查询去相关。
- Procedure 和算法边界识别。

### 7.3 CBO

采用 Memo/Cascades 风格优化框架。统计包括：

- Label、Edge Type 和时间桶基数。
- 属性选择率。
- 入度、出度直方图及高度点 Top-K。
- 跨 Shard 边比例。
- 每元素版本数和 Delta Chain 长度。
- 区间长度、重叠度和 Coalesce 压缩比。
- Adapter Operator 延迟、吞吐和失败率。
- 网络、序列化、FFI 和远程 Provider 成本。

物理选择包括 Scan/Seek、Current/History、Expand 方向、Join、Local/Broadcast/Repartition、Pushdown 和算法替换。

运行时自适应只允许在 `AdaptiveBoundary` 发生，并保持相同 Snapshot Token。

## 8. Backend Capability 与下推

```rust
BackendCapabilities {
    point_lookup,
    multi_get,
    ordered_scan,
    property_filter,
    temporal_filter,
    adjacency_expand,
    joins,
    aggregation,
    transactions,
    prepare_commit,
    snapshot_reads,
    change_feed,
    exact_semantics,
    limits,
}
```

Pushdown 等级：

- `Exact`：后端结果与 IR 语义完全相同。
- `LosslessCandidate`：后端产生无漏失候选，DTGProxy 执行 Residual Filter。
- `Approximate`：只允许显式近似算法。

后端映射：

- RocksDB：MultiGet、Prefix/Range Scan、邻接和时间索引扫描。
- Neo4j/Memgraph：Label/Type Scan、属性过滤、局部 Expand 和候选子图提取。
- PostgreSQL：Index Scan、Filter、Join、Aggregate、Sort 和 CTE。

后端能力变化使相关 Plan Cache 失效。普通查询不得把 Approximate 结果当作精确结果。

后端不需要原生理解双时态语义。Version Rewriter 在提交前生成确定性的 Current Projection、History Anchor/Delta、邻接、约束和时态索引 Mutation；Replica 按相同 Raft 顺序应用。查询读取 Current Projection 或在指定 transaction time 下重建 History Projection，再由 Temporal IR 执行区间匹配和聚合。因此输入是双时态图、落盘可以是 KV、普通属性图或关系表，读出后仍恢复成相同的双时态图。

## 9. 分布式查询运行时

### 9.1 组件

- Gateway：协议、认证、会话和结果编码。
- Query Coordinator：DAG、Stage、Exchange、重试和取消。
- Temporal Transaction Coordinator：Snapshot、Read/Write Set 和提交。
- Shard Worker：本地算子和 Backend Fragment。
- Exchange Service：批量跨分片 Arrow RecordBatch。
- Admission Controller：资源与优先级。
- Recovery Worker：遗留事务和 Intent 恢复。

### 9.2 Snapshot Token

```rust
SnapshotToken {
    graph_id,
    tenant_id,
    start_ts,
    transaction_as_of,
    valid_selector,
    schema_version,
    partition_epoch,
    placement_version,
    consistency,
    security_fingerprint,
    snapshot_lease_id,
}
```

所有 Fragment 必须返回相同 Token。Shard 只有在 `safe_ts >= transaction_as_of` 时可读，其中：

```text
safe_ts = min(closed_ts, resolved_ts, adapter_applied_ts)
```

### 9.3 Physical DAG

核心物理算子：

- `LocalFragment`、`BackendFragment`。
- `HashExchange`、`RangeExchange`、`BroadcastExchange`。
- `Gather`、`MergeGather`。
- `LocalPartialAggregate`、`GlobalAggregate`。
- `Barrier`、`Checkpoint`、`Materialize`、`Spill`。
- `AdaptiveBoundary`。

执行原则：

- Frontier 按目标 Shard 分桶并批量传输。
- Local Partial Aggregate、TopN 和 Bloom Filter 靠近数据执行。
- Arrow RecordBatch 流式传输，使用 Credit-Based Backpressure。
- 标准查询不返回缺失 Shard 的部分结果。
- 无 `ORDER BY` 时不承诺行序；有 `ORDER BY` 时使用 Local Sort + Merge Gather。
- Raft Apply、事务、OLTP 查询和 OLAP 使用隔离的线程池与 IO 配额。

### 9.4 两种部署模式

PrimaryReplica：

- 整图可位于一个逻辑 Shard Group。
- 写入 Primary，单 Shard 使用 1PC。
- Snapshot/History Read 可路由到满足 safe timestamp 的 Replica。

Shared-Nothing：

- 图分布到多个 Shard Group，每组内部仍使用 Raft 复制。
- 顶点有唯一 Home Shard。
- 出入邻接、索引和约束键按确定规则分片。
- 跨 Shard 写入使用时态 2PC。
- 高度点使用 Adjacency Directory 和稳定邻接桶。

两种模式只改变 Placement 和 Exchange，不改变 Logical IR、事务 API 和错误语义。

## 10. Cypher 写语义与时态事务

### 10.1 Transaction Overlay

```text
TxnOverlay {
    deterministic_mutations,
    created_elements,
    deleted_elements,
    property_overrides,
    adjacency_additions,
    adjacency_removals,
    constraint_claims,
}
```

Backend 返回 start snapshot，`OverlayMerge` 将事务内未提交变更覆盖到快照上，实现 Read-Your-Writes。写操作先进入 Version Rewriter，不直接修改 Backend。

Planner 在读写依赖处插入 `MutationBarrier`、`EagerMaterialize`、`OverlayMerge` 和 `ConstraintCheck`。

### 10.2 Auto-Commit

```text
RUN -> implicit begin -> execute -> PULL/DISCARD -> commit -> summary/bookmark
```

写查询在结果消费完成或 DISCARD 后提交。客户端断开时，COMMITTED 前可回滚；COMMITTED 后必须继续 Roll Forward。

### 10.3 隔离级别

Temporal Snapshot Isolation：

- 事务固定 start timestamp。
- 检测普通写写冲突。
- 同一元素上重叠 valid interval 的写入冲突。
- 强制唯一性、端点存在和版本不重叠。

Temporal Serializable：

- 额外记录元素键、索引范围、邻接前缀、时间谓词和路径 Predicate Token。
- Prepare 验证 `(start_ts, commit_ts]` 中没有覆盖 Read Set 的写入。

### 10.4 2PC

```text
ACTIVE -> PREPARING -> COMMITTED -> APPLIED
                 \-> ABORTED -> CLEANED
```

流程：

1. Begin 从 TSO 获取 start timestamp。
2. 查询执行动态发现参与 Shard。
3. Version Rewriter 生成确定性 Mutation。
4. PREPARING 前冻结参与者集合。
5. 并行 Prewrite 并验证 Snapshot、Schema、Epoch、Intent、时态冲突和约束。
6. Intent 与 Mutation 摘要先写入各 Shard Raft。
7. TSO 分配大于所有下界的 commit timestamp。
8. Home Shard 写入 COMMITTED 决议和参与者证明。
9. Home Record 经多数提交后成为不可逆点。
10. 所有参与者 Finalize，Adapter 物化并推进 applied timestamp。

单 Shard 事务把验证、提交记录和 Mutation 放入一次 Raft Entry，使用 1PC。

### 10.5 MERGE 与完整性

`MERGE` 通过 Constraint Key 路由到确定性 Constraint Shard 并获取 Intent，不实现为无保护的“先查后建”。边创建同时保护 Edge Home、双向邻接、Endpoint Guard 及相关索引。顶点删除获取 Endpoint Guard 排他 Intent。

## 11. 故障、重试与再均衡

- Fragment 网络故障可在相同 Token 下透明重试。
- Epoch 或计划失效时可保持 Snapshot Token 整体重建 DAG。
- 写事务 PREPARING 前发现 Epoch 变化时整体重试。
- PREPARING 后不得动态改变参与者或转发到新 Shard。
- COMMITTED 前 Coordinator 故障可依据 Home Record 和 TTL 回滚。
- COMMITTED 后任何参与者或 Recovery Worker 只能 Roll Forward。
- 客户端超时不等于回滚；客户端用 txn ID 查询最终状态。
- Adapter Apply 失败时停止推进 safe timestamp，并从已提交日志恢复。
- 查询遇到 Intent 时必须查 Home Record、等待、帮助提交或清理，禁止忽略。

错误分为透明重试、整体查询重试、事务冲突、Snapshot Not Ready、不确定提交、永久错误和资源错误。错误码稳定并映射 Bolt 状态。

## 12. 时态图分析运行时

### 12.1 图投影

`SnapshotGraph`：一个 valid/transaction time 点上的 CSR/CSC 普通图。

`IntervalGraph`：节点、边和属性携带有效区间。

`EventGraph`：边是带 event time、duration 和 weight 的事件。

`DeltaGraph`：两个视图之间的新增、删除和属性变化。

事务时间只冻结系统认知；算法时间轴使用有效时间或事件时间。

### 12.2 时态遍历语义

```rust
TemporalTraversalSemantics {
    edge_model: Event | Interval,
    time_order: Strict | NonDecreasing,
    waiting: Allowed | Forbidden,
    traversal_duration_property,
    path_mode: Walk | Trail | Acyclic,
    objective,
}
```

存在歧义且请求未声明完整语义时拒绝执行。

### 12.3 Algorithm Catalog

每个算法声明：

- 名称、版本和别名。
- 支持的图模型、方向、多重边和自环。
- Weight 类型和负权策略。
- 时态遍历语义。
- 精确/近似、确定性、分布式和增量能力。
- 输出 Schema、复杂度提示和 Provider 需求。

### 12.4 调用

同步：

```cypher
CALL dtg.temporal.earliestArrival({
  graph: 'accounts',
  source: $source,
  validFrom: $from,
  validTo: $to,
  transactionAsOf: $tx,
  waiting: true,
  timeOrder: 'NON_DECREASING'
})
YIELD vertexId, arrivalTime, predecessor
RETURN *
```

大任务使用 `dtg.analytics.submit`、`status`、`results` 和 `cancel` 过程。

### 12.5 Provider SPI

```rust
trait AnalyticsProvider {
    fn descriptor(&self) -> ProviderDescriptor;
    fn algorithms(&self) -> Vec<AlgorithmDescriptor>;
    fn validate(&self, graph: &ProjectedGraphManifest,
                request: &AlgorithmRequest) -> ValidationResult;
    fn submit(&self, request: AlgorithmRequest) -> JobHandle;
    fn status(&self, job: &JobHandle) -> JobStatus;
    fn results(&self, job: &JobHandle) -> ResultStream;
    fn checkpoint(&self, job: &JobHandle) -> CheckpointRef;
    fn cancel(&self, job: &JobHandle);
}
```

Provider 选择先验证语义和精确度，再比较图规模、分布/GPU 需求、数据格式、投影成本、健康状态和队列。

## 13. 算法范围

### 13.1 Analytics 1.0 普通图算法

- BFS、SSSP、WCC、SCC。
- PageRank、Degree Centrality。
- Triangle Count、Clustering Coefficient。
- K-core、LPA。

### 13.2 Analytics 1.0 时态算法

- Temporal Reachability。
- Earliest Arrival、Latest Departure。
- Fastest、Minimum-Hop Temporal Path。
- Temporal Closeness、Temporal Betweenness。
- Temporal PageRank、Temporal Degree。
- Burstiness、Topological Overlap。
- Temporal Clustering Coefficient。

### 13.3 后续算法

- Temporal K-core、Temporal Motif。
- Community Evolution、Louvain、Leiden。
- Incremental PageRank/WCC/K-core。
- Sliding-Window Triangle/Motif。
- Temporal Random Walk、Link Prediction 和图学习投影。

## 14. 第三方源码与集成边界

| 项目 | 许可证/约束 | 集成方式 | 决策 |
|---|---|---|---|
| openCypher | Apache-2.0 | BNF/TCK | 规范及测试必用 |
| Grafeo | Apache-2.0 | 审计后抽取 Parser/CST/AST | 候选 |
| Apache AGE | Apache-2.0/PostgreSQL 耦合 | Binder/错误语义参考 | 不嵌入 |
| Kùzu | MIT/已归档 | Planner 参考 | 不作为核心依赖 |
| petgraph | Apache-2.0/MIT | 固定稳定 Cargo 版本 | 直接采用 |
| TGLib | MIT/C++ | 固定 Commit、C ABI/Sidecar | 首要时态算法来源 |
| LAGraph | BSD-2-Clause/C | 稳定 C API | 可选 CPU Provider |
| SuiteSparse GraphBLAS | Apache-2.0/C | LAGraph 依赖 | 可选 |
| GraphScope | Apache-2.0/重型分布式栈 | Remote Provider | 采用 |
| cuGraph | Apache-2.0/CUDA | Remote/GPU Provider | 可选 |
| Differential Dataflow | MIT/Rust | 独立增量 Provider | 后期采用 |
| Raphtory | GPL-3.0 | 独立服务或论文参考 | 禁止链接/复制 |
| Memgraph/MAGE | BSL 等 | 行为参考 | 默认禁止复制 |

TGLib 首期通过连续 Arrow Buffer -> C ABI -> Arrow ResultBatch 集成。使用 Dense ID 映射、64 位时间单位、固定 Commit 和校验和。生产部署可用 Sidecar 隔离 Native 崩溃。热点时态算法再选择性移植到纯 Rust。

第三方清单存放在：

```text
third_party/
├── manifest.toml
├── licenses/
├── patches/
└── notices/
```

首次引入前执行最低许可证门禁；完整 Boundary/SBOM 审核在发布前执行。

## 15. DTG Native Distributed Analytics

提供无需外部 GraphScope 的核心分布式运行时：

- Vertex-Centric Superstep 和 Scatter/Gather。
- Message Combiner、Push/Pull 动态切换。
- Master/Ghost Vertex 同步。
- Frontier Bitmap 和 Local GraphBLAS Kernel。
- Global Barrier、Checkpoint/Resume。
- Deterministic Reduction 和 Delta Superstep。

它与查询运行时共用 Snapshot、Shard 和 Exchange 协议，但使用独立资源池。GraphScope 继续承担成熟的大规模 PIE/Pregel/FLASH 等场景。

## 16. 增量分析与结果元数据

```text
Base Snapshot S0
 -> Initial Result R0
 -> consume CDC (S0, S1]
 -> incremental update
 -> Checkpoint R1 + per-shard CDC offsets
```

结果携带：

- `derived_from_snapshot`、`through_commit_ts`。
- valid window、algorithm/provider version。
- parameter hash、random seed。
- exactness、coverage 和 convergence。

只有所有目标 Shard 的 CDC Watermark 到达同一 commit timestamp，才能发布全局一致结果。

## 17. 缓存与资源隔离

缓存：

- Prepared Plan Cache。
- Current Entity/Adjacency Cache。
- History Reconstruction Cache。
- Snapshot/Interval/Event/Delta Projection Cache。
- CSR/CSC、GraphBLAS Matrix、GraphAr 和 GPU Graph Cache。
- Algorithm Result Cache。

所有缓存键包含必要的 Graph、Schema、Snapshot、Temporal Selector、Partition Epoch、Capability 和安全指纹。

每个查询/算法声明 deadline、priority、memory、spill、network、rows、frontier、history versions 和 participant shards 上限。

调度优先级：

```text
Raft Apply/Recovery
  > OLTP Write
  > OLTP Point Query
  > Interactive Traversal
  > Historical Scan
  > Analytics
  > Background Compaction
```

## 18. Crate 演进

新增：

```text
cypher-syntax
cypher-ast
cypher-sema
cypher-compiler
bolt-protocol
bolt-server
query-optimizer
physical-plan
distributed-query
procedure-runtime
analytics-api
graph-projection
analytics-runtime
analytics-native
provider-petgraph
provider-tglib
provider-lagraph
provider-graphscope
incremental-analytics
```

调整：

- `temporal-query` 保留旧 DSL 作为调试/兼容入口。
- `temporal-ir` 升级为版本化 v2，并保留 v1 解码。
- `query-executor` 逐步拆成 Local Runtime 与 Distributed Coordinator。
- `gateway-node` 接入 Bolt。
- `txn-protocol` 增加 Query Overlay 和 Statement Context。
- `storage-api` 增加 Projection、Capabilities 和 Fragment。

## 19. 里程碑

M0 规范与测试基础：兼容档、Temporal 语法、TCK、Bolt 测试、差分工具和第三方清单。

M1 Cypher 前端与参考解释器：Parser、Binder、类型、Temporal IR v2 和未优化 Oracle。

M2 完整读查询与 Bolt：读 Clause、路径、Procedure、RBO、Plan Cache 和三后端一致性。

M3 写查询与事务：完整写 Clause、Overlay、Auto-Commit、TSI/Serializable、1PC/2PC。

M4 分布式查询：Physical DAG、Exchange、分布式 Join/Aggregate/Sort、CBO 和 Adaptive Boundary。

M5 本地时态分析：四类 Projection、petgraph/TGLib/LAGraph 和 Analytics 1.0。

M6 分布式与增量分析：DTG Native、GraphScope/cuGraph、CDC 和 Checkpoint。

M7 完整认证：兼容、Chaos、长稳、性能、安全、Boundary 和 SBOM。

## 20. 稳定性状态

```text
EXPERIMENTAL -> PREVIEW -> STABLE -> DEPRECATED -> REMOVED
```

进入 Stable 必须具备冻结规范、正确性测试、跨后端差分、分布式故障测试、性能基线、错误码、可观测性、升级策略和许可证清单。

## 21. 性能门槛

### 21.1 查询

- Prepared Plan Cache 命中时规划开销目标不超过查询总延迟 5%。
- 常用参数化查询 Cache 命中率目标大于 95%。
- Current Point Lookup 相对直接 Adapter 的额外 p99 延迟目标不超过 15%。
- Current 一跳遍历额外开销目标不超过 20%。
- Parser/Planner 成本不得随元素历史版本数线性增长。

### 21.2 分布式

- 可分区负载 4 节点吞吐不低于单节点 2.8 倍。
- 可分区负载 8 节点吞吐不低于单节点 5 倍。
- 高度点查询的 Coordinator 内存受预算约束。
- Projection、网络、Provider 排队和算法执行分开报告。

### 21.3 算法

- 执行超过 1 秒的算法，Provider 调用开销目标低于 10%。
- 8 节点分布式算法有效扩展效率目标不低于 60%。
- 确定性和快速模式分别报告。
- 近似算法报告误差、采样、覆盖率、随机种子和收敛。

所有指标必须给出硬件、数据集、配置、版本深度和原始结果；本规范中的数字是发布门槛，不是已有性能声明。

## 22. 测试与验收

### 22.1 测试层级

每次提交：Unit、Parser/IR Golden、Lint、Format、License Dependency Check。

每日：Backend Integration、Cypher Differential、Driver Compatibility、Algorithm Oracle。

每周：Distributed Chaos、Transaction Recovery、Fuzz、Performance、Leak。

发布前：完整兼容矩阵、三后端认证、4/8 节点扩展、安全和 Boundary/SBOM。

### 22.2 Stable 验收矩阵

| 维度 | 门槛 |
|---|---|
| Cypher 5 | 冻结兼容面全部通过 |
| Cypher 25 | 对应日期基线全部通过 |
| Temporal Cypher | Temporal TCK 全部通过 |
| Bolt | 支持矩阵中的官方 Driver 全部通过 |
| 后端 | RocksDB、Neo4j、PostgreSQL 规范结果一致 |
| PrimaryReplica | 故障切换不破坏 Snapshot/Bookmark |
| Shared-Nothing | 不混合 Snapshot/Epoch，不返回缺失 Shard |
| 事务 | TSI/Serializable 历史检查无异常 |
| 恢复 | COMMITTED 后最终 Roll Forward |
| 精确算法 | 与小图 Oracle 一致 |
| 近似算法 | 误差和覆盖率符合声明 |
| 增量算法 | 与相同 Snapshot 全量重算一致 |
| 性能 | 达到发布目标且无显著回退 |
| 资源 | Memory/Network/Spill/Timeout/Cancel 受控 |
| 安全 | 权限、Procedure 沙箱和输入 Fuzz 通过 |
| 许可证 | SBOM、NOTICE、第三方清单完整 |

### 22.3 形式化与故障验证

- 用 TLA+ 或等价模型验证 2PC、safe timestamp 和 Epoch 切换。
- 用 Loom 验证 Rust 并发状态。
- 用 Jepsen 风格历史验证 Snapshot Isolation 和 Serializable。
- 故障注入覆盖 Gateway、Coordinator、TSO、Meta、Leader、Adapter 和网络分区。
- 优化规则使用优化前后等价性测试。
- 分布式算法与单机参考结果或声明误差对比。

## 23. 可观测性

一次查询、事务或算法共享 Trace ID。Span 至少覆盖：

- Bolt/Gateway。
- Parser、Binder、Planner、Optimizer。
- Coordinator 和每个 Stage。
- Shard、Raft propose/apply。
- Adapter 和底层后端。
- Projection、Provider、Checkpoint 和 Result Store。

`EXPLAIN ANALYZE` 返回脱敏后的计划、估计/实际基数、历史版本展开量、网络字节、Spill、Provider 排队和各阶段耗时。

## 24. 实施规范拆分

本文件是总架构规范。实现前拆成四条独立计划：

1. Cypher/Bolt 前端。
2. Temporal IR、优化器与分布式查询。
3. Cypher 写语义与分布式时态事务。
4. 时态分析运行时与算法 Provider。

各计划共享本文件中的 Snapshot、Temporal Row、Capability、错误、版本和 Stable Gate，不得自行定义冲突语义。

## 25. 参考资料

- [openCypher Grammar and TCK](https://github.com/opencypher/openCypher)
- [Neo4j Cypher Version Selection](https://neo4j.com/docs/cypher-manual/current/queries/select-version/)
- [Neo4j Bolt Protocol](https://neo4j.com/docs/bolt/current/bolt/)
- [GraphScope GIE Design](https://graphscope.io/docs/interactive_engine/design_of_gie)
- [GraphScope GOpt](https://graphscope.io/docs/interactive_engine/gopt)
- [GraphScope Analytical Algorithms](https://graphscope.io/docs/analytical_engine/builtin_algorithms)
- [NebulaGraph Query Engine](https://docs.nebula-graph.io/1.2.0/manual-EN/1.overview/3.design-and-architecture/3.query-engine/)
- [TigerGraph Distributed Query Mode](https://docs.tigergraph.com/gsql-ref/4.2/querying/distributed-query-mode)
- [Galaxybase Product Architecture](https://www.galaxybase.com/galaxyproduct)
- [TGLib](https://tgpublic.gitlab.io/tglib/index.html)
- [petgraph](https://github.com/petgraph/petgraph)
- [LAGraph](https://github.com/GraphBLAS/LAGraph)
- [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow)
- [Raphtory](https://github.com/Pometry/Raphtory)

资料说明：本规范的外部项目事实由 AI 辅助检索官方仓库和官方文档后综合；实际引入第三方代码前必须重新锁定 Commit、递归依赖、许可证和安全状态。
