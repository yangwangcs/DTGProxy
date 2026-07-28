# DTGProxy T-Cypher Clean-Break 设计规范

日期：2026-07-23

状态：已批准的唯一权威查询设计

取代：`2026-07-19-dtgproxy-temporal-cypher-analytics-design.md`

来源参考：Cedar `b37d3a2` 及其 Apache-2.0 T-Cypher、向量化执行与混合路径设计。

## 1. 决策与目标

DTGProxy 定义并版本化本规范第 3 至第 6 节的 T-Cypher 语法和时态语义，并将其实现为分布式、后端无关的 Rust 语言层。Cedar `b37d3a2` 仅作为语义来源、算法出处和实现参考，不是运行时依赖、类型依赖或后续语言版本的自动权威。迁移是 clean-break：系统只有一套 Parser、AST、Binder、Temporal IR、物理计划和生产执行器，不保留旧语法、旧计划解码器或兼容分支。

唯一时态语言使用：

- `FOR VALID_TIME AS OF/BETWEEN`；
- `FOR SYSTEM_TIME AS OF`；
- `CHANGES FOR VALID_TIME/SYSTEM_TIME BETWEEN`；
- `VALID FROM` 时态写入；
- `valid_from()`、`valid_to()`、`system_time()`、`commit_seq()`、`operation()`。

以下内容从生产代码、测试、示例和文档中删除：

- `AT VALID_TIME`；
- `AT TRANSACTION_TIME`；
- `DIFF GRAPH`；
- `DiffStatement`、`LogicalOperator::Diff` 及只服务旧语义的类型；
- 任何 V1/V2、legacy、compat 查询路径。

普通 Cypher 仍是 T-Cypher 的非时态子集。`USE graph`、Bolt、分布式事务、Procedure 和分析 API 保持存在，但全部通过本规范的唯一查询管线。

## 2. 迁移边界与源码复用

Cedar 是 C++17 单机 HTAP 存储内核；DTGProxy 是 Rust 分布式中间件，不能链接或整体嵌入 Cedar 查询运行时。采用三类复用：

1. 直接语义复用：语法、半开区间、事件可见性、路径区间、`TRAIL`、需求驱动切分、支持矩阵。
2. 审计后算法移植：`interval_derive`、`interval_align`、`temporal_coalesce` 的独立纯函数，改写为 DTGProxy 类型并保留 Apache-2.0 出处。
3. 架构吸收：ColumnBatch、morsel、内存账户、spill、取消、算子指标；在现有 Rust 模块内实现，不复制 C++ 对象模型。

不复用 Cedar 的存储、事务、WAL、SST、SchemaRegistry、CBO 或调度器。DTGProxy 的 canonical temporal KV、Raft、2PC、后端 Adapter 和分布式 Snapshot 是权威基础。

## 3. 唯一语言语义

### 3.1 查询级作用域

```cypher
USE accounts
FOR SYSTEM_TIME AS OF $tx
FOR VALID_TIME AS OF $valid
MATCH (a:Account)-[r:TRANSFER]->(b:Account)
RETURN a, r, b
```

未指定 system time 时，使用语句开始时固定的全局可见提交前缀；未指定 valid time 时，使用语句开始时固定的物理时间。`now()` 每条语句只求值一次。

`FOR VALID_TIME BETWEEN $from AND $to` 返回与查询窗口相交的最大状态区间。返回的 `valid_from` 和 `valid_to` 是事实的真实边界，不裁剪为查询边界。所有区间为 `[start,end)`；空区间或反向区间在绑定阶段报错。

### 3.2 MATCH 级覆盖

查询级作用域是默认值，每个 `MATCH` 可以覆盖 valid/system time：

```cypher
FOR VALID_TIME AS OF $t1
MATCH (a:Person)
MATCH (b:Person) FOR VALID_TIME AS OF $t2
WHERE a.id = b.id
RETURN a, b
```

同一 pattern 内的节点和边共享一个时态作用域，禁止元素级互不相干的时间。多个 `MATCH` 可以使用不同 system-time cutoff，但都不得超过语句捕获的可见前缀。

### 3.3 变化查询

```cypher
CHANGES FOR VALID_TIME BETWEEN $from AND $to
MATCH (n:Person)
RETURN n, operation(n), commit_seq(n)
```

- valid-time changes 选择 `valid_from` 位于窗口内的不可变事件；
- system-time changes 选择提交 HLC 位于窗口内的事件；
- 一条语句只有一个 change axis；
- `CHANGES` 返回事件，不重建状态；
- 混合定长/变长路径的 `CHANGES` 没有无歧义事件归属，本版稳定拒绝。

### 3.4 时态写入

```cypher
CREATE (n:Person {name: 'Li'}) VALID FROM $t
MATCH (n:Person {id: $id}) SET n.name = 'Wang' VALID FROM $t
MATCH (n:Person {id: $id}) DELETE n VALID FROM $t
```

用户只提供 `valid_from`；`valid_to` 由相同 logical key 的下一可见事件推导。`VALID TO` 永远非法。历史回填创建新事件，不原地覆盖旧 transaction-time 历史。写入的 system time 只能由 DTGProxy TSO 在提交时分配。

## 4. AST、Binder 与 Temporal IR

AST 使用正交类型：

```text
TemporalAxis = ValidTime | SystemTime
TemporalMode = StateAsOf | StateBetween | ChangesBetween
TemporalScope { axis, mode, start, end? }
```

`QueryStatement` 持有查询默认 scope；`MatchClause` 持有可选覆盖 scope。变化查询不是独立旧式 `DiffStatement`，而是同一查询 AST 上的 `ChangesBetween` scope。

Binder 负责：

- 作用域继承与重复/冲突检查；
- 参数、timestamp literal 和稳定时间函数的类型；
- system-time 历史写禁止；
- 元数据函数的 provenance 归属；
- 变长路径必须具有有限且合法的 hop 范围；
- 变长关系绑定为 `LIST<RELATIONSHIP>`，不能直接读取 `p.weight`；
- `CHANGES` 的单一 change axis 和不支持组合；
- 写语句 `VALID FROM` 和事务隔离合法性。

Temporal IR 删除 `Diff`，增加显式算子：

- `TemporalPointScan`、`TemporalRangeScan`、`ChangeScan`；
- `IntervalDerive`、`IntervalAlign`、`TemporalCoalesce`；
- `Expand`、`BoundedVariableExpand`、`PathTrail`；
- `PropertyGather`、`MetadataProject`；
- 现有关系、写、Procedure 和分布式算子。

计划指纹固定语言语义版本、Schema、所有 temporal scope、参数类型、事务快照、Topology Epoch、后端 capability generation 和安全主体。不存在旧计划反序列化。

## 5. 双时态正确性内核

### 5.1 可见事件与区间推导

对状态重建中的一个 logical key 和 system snapshot：先丢弃 commit timestamp 大于 cutoff 的事件；同一 `valid_from` 选择最大可见 commit；按 `valid_from` 排序，下一事件的起点推导当前 `valid_to`，末项为正无穷。状态匹配忽略 DELETE。`CHANGES` 不执行该状态折叠，而是按 canonical event-log 补充规范保留窗口内每条不可变 PUT/DELETE 及其原始 interval/provenance。

区间扫描必须读取查询左边界的可见 predecessor，以及推导最后区间所需的窗口右侧第一个 successor。

### 5.2 对齐与合并

需求集合包括存在事实、谓词/投影/分组/排序使用的属性、完整返回实体的全部属性和显式 provenance。运行时收集这些事实的边界，将它们切为非重叠 cell；每个 cell 只有在所有必需事实可见且图约束成立时产生结果。

相邻 cell 仅当所有用户可见值相同才可合并；查询显式要求 provenance 时，commit、operation、schema 等 provenance 也必须相同。未被引用的属性变化不能切分结果。

### 5.3 图与路径可见性

边的有效区间是边存在与两个端点存在区间的交集。完整路径的区间是查询候选域、所有节点、所有边和所有 demanded property 区间的交集。空交集不返回。

## 6. 混合路径与 TRAIL

支持单个 pattern 内任意混合的定长和有界变长 segment：

```cypher
MATCH (a)-[p:KNOWS*1..3]->(b)-[r:WORKS_WITH]->(c)
RETURN a, p, b, r, c
```

每个 segment 有独立的 `[min_hops,max_hops]`；定长为 `[1,1]`。执行使用 segmented frontier：固定 segment 前进一步，变长 segment 从 hop 1 迭代至上限，并从最小 hop 开始把完成状态交给下一 segment。

visited-edge set 跨 segment 传递，因此 `TRAIL` 对完整 pattern 生效。边不可重复，节点允许重复。每一步同步求交时态域并在空域时提前裁剪。变长关系绑定返回有序关系列表，固定关系绑定返回 Relationship。

frontier 按目标 shard 分桶；跨分片状态携带 Snapshot Token、segment index、hop、path edge IDs、全局 visited set、时态域和必要 binding。所有边界有数量、字节、hop 和 participant 上限。

## 7. 唯一向量化运行时

逻辑和物理计划 schema 由稳定 slot ID、ValueType 和 nullability 定义，与行式或列式布局无关。`ColumnBatch` 只在物理执行 lowering 后绑定这些 slots：固定宽度列使用 typed vectors 和 validity bitmap；String/Bytes/List/Map/Node/Relationship/Path 使用 offset + arena/reference。行式表示仅允许测试 oracle、Bolt 单值编码和 Adapter 边界，不得成为生产算子间协议。`cypher-syntax`、`cypher-ast`、`cypher-sema`、`cypher-compiler`、`temporal-ir` 和 `storage-api` 禁止依赖 `ColumnBatch` 或 `ColumnVector`。

```text
T-Cypher -> AST -> Bound AST -> Temporal Logical IR
         -> Optimized IR -> Distributed Physical DAG
         -> typed backend primitives -> bounded canonical pages
         -> Adapter boundary codec -> ColumnBatch vector pipelines
```

这里的 `ColumnBatch` 是 DTGProxy 自己的执行器 ABI，不是 Adapter SPI、后端存储模型或语言
语义。KV、行存、列存、原生图数据库和远程 Sidecar 都只需实现同一组 typed、bounded、
capability-aware primitives；后端可以采用任意内部布局。语言层只绑定图、时态、值类型、
slot 和 provenance，不能根据后端是行式或列式改变查询含义。

流水线以 morsel 调度。Scan/Expand/Filter/Project 尽量融合；Join/Aggregate/Distinct/Sort/Coalesce 是显式 pipeline boundary。PropertyGather 在候选和区间裁剪之后执行，避免读取被淘汰行的属性和大值。

分布式 Exchange 使用独立、版本化的 canonical columnar wire encoding；`ColumnBatch` 通过显式 codec 与该格式互转，内存布局不构成网络 ABI。wire payload 携带 schema fingerprint、batch sequence、checksum 和 Snapshot Token。不得把用户原始 T-Cypher 下推给后端。

## 8. 内存、spill、取消和指标

每个查询有层级内存账户：query -> pipeline -> operator。批次、哈希表、frontier、visited set、属性 arena、排序/聚合状态、交换缓冲和结果缓冲必须先 reserve 后分配。

支持 spill 的阻塞状态包括 Sort、Hash Join build、Aggregate、Distinct、Coalesce、variable frontier 和物化结果。spill：

- 写入查询私有目录，文件名不含用户文本；
- 使用版本、schema fingerprint、chunk length、checksum；
- 分区有界且读写计入 IO budget；
- cancellation、失败和正常结束都清理；
- 不属于数据库持久状态，不能用于事务恢复。

不支持 spill 的分配在超限时返回稳定资源错误，禁止无界增长或静默降级为全量物化。

取消检查至少位于 morsel、segment transition、frontier partition、网络等待、spill IO、property gather 和输出批次边界。写查询在提交不可逆点前取消则 abort；不可逆点后 roll forward 并返回可查询的事务结果。

`EXPLAIN ANALYZE` 为每个算子报告输入/输出行、批次、时间、峰值内存、spill 次数/字节、网络字节、后端等待、区间切分/合并数量。变长 Expand 额外报告 hop、frontier、partition、completed paths 和最大 frontier。

## 9. 分布式快照与事务

每条语句捕获不可变 `SnapshotToken`，包含 graph/tenant、visible commit ceiling、system-time cutoff、statement valid time、schema、partition/placement epoch、backend generation、安全指纹和租约。所有 fragment 必须验证相同 token。

Shard 仅在 `safe_ts = min(closed_ts,resolved_ts,adapter_applied_ts)` 覆盖 cutoff 时执行。不同 `MATCH` 的历史 system-time scope 可低于 ceiling，不可高于 ceiling。拓扑变化只能依据 lineage 证明同一逻辑 snapshot 后重建 DAG，不能拼接 epoch。

写入继续走 canonical temporal mutation -> Version Rewriter -> Shard Raft -> Adapter materialization。单 shard 使用 1PC；多 shard 使用 durable temporal 2PC。参与者在 PREPARING 前冻结；commit timestamp 大于 start 和所有 participant lower bound；COMMITTED 后只可 roll forward。Transaction Overlay 为 T-Cypher 提供 read-your-writes。

Cedar 的单机事务代码不迁移，DTGProxy 现有分布式事务语义保持权威。

## 10. 三后端统一映射

RocksDB、Neo4j 和 PostgreSQL 只持久化 canonical temporal model 的后端映射，不定义查询语义：

- RocksDB：规范 KV、时间/邻接前缀和批量 MultiGet/Range Scan；
- Neo4j：受管理节点/关系及版本记录，使用属性/label/type 候选扫描；
- PostgreSQL：规范实体、事实、邻接和索引关系表，使用参数化 SQL；
- 所有候选必须回到 DTGProxy 执行 residual temporal semantics；
- DTGProxy 1.1 即使 capability 标为 Exact 也保留 residual；只有完成独立等价性证明后，后续版本才可省略；
- backend generation/capability 改变使计划失效。

Adapter SPI 增加批量 point/range/change scan、batch property gather 和 adjacency expand。SPI 返回有界且带 snapshot/capability 标识的存储域候选页或事件页，不返回 `ColumnBatch`，也不得依赖 `query-executor`；`ColumnBatch` 转换由查询运行时在 Adapter 边界完成。后端不得接收原始查询，不得自行选择 system-time snapshot，不得返回未标识 snapshot 的结果。三后端读取必须由同一表示无关 temporal oracle 做差分。

一次性 `FencedScan` 与可复用 `ReadSnapshot` 是两个不同 capability。`FencedScan` 只保证一个有界扫描结果与其 `applied_log_index` 精确对应，可用于只执行一次 canonical event-index scan 的 `CHANGES` source；分页、point/range 混合读取或多个 backend primitive 的 fragment 必须使用真实 `ReadSnapshot` 或 begin/read/end session token。禁止用全量物化、全局最高已观察索引或 single-use 对象伪装可复用 snapshot。

## 11. Crate 落点

- `cypher-syntax`：唯一 `FOR`/`CHANGES`/`VALID FROM` grammar，删除旧 token/parser。
- `cypher-ast`：正交 temporal scope、MATCH override、metadata expressions，删除 Diff AST。
- `cypher-sema`：scope、类型、路径、change-axis 和历史写约束。
- `cypher-compiler`：FactDemandSet、区间算子、mixed segment 和 metadata lowering。
- `temporal-ir`：唯一当前逻辑 IR，不含 Diff/legacy/version adapter。
- `physical-plan`：typed column slots、segment expand、pipeline、spill/cancel/metrics contract。
- `temporal-semantics`：表示无关的区间推导、对齐、coalesce 判等和事件可见性纯函数；只依赖 `temporal-types`，不得依赖 `query-executor`、`ColumnBatch` 或 Adapter。
- `query-executor`：ColumnBatch、向量算子、frontier、memory/spill，并调用表示无关的时态语义核心。
- `distributed-query`：columnar Exchange、分片 frontier 和 token fencing。
- `cypher-engine`/`gateway-node`：Bolt、事务和结果流只接入新 compiler/runtime。
- `storage-api` 与 adapters：批量 primitives 和 capability，不感知语言。

分析 Procedure 使用同一新时态 scope 生成 SnapshotGraph、IntervalGraph、EventGraph 或 DeltaGraph。现有算法账本、checkpoint、跨 Gateway 接管和 Artifact GC 不重写，只更新查询语法与投影输入。

## 12. Clean-Break 删除规则

迁移按纵向切片推进，但主分支最终不得同时存在两个入口。删除门禁：

1. `rg` 不得在生产源码、测试查询和用户文档中找到旧语法；负面迁移测试可保存字符串并必须断言拒绝。
2. 公共 enum/type 不得含 `DiffStatement`、`Diff` 或 `TransactionTime` 语言命名；内部存储类型可继续称 transaction time。
3. 不得出现 V1/V2 parser、legacy compiler、compat executor、fallback。
4. 所有 examples、TCK、Bolt、三后端和故障测试只使用新语法。
5. 旧设计文件被本文件取代并删除；实施计划只能引用本文件。

不提供自动语法重写器。旧查询返回稳定 parse error，提示使用 `FOR SYSTEM_TIME`、`FOR VALID_TIME` 或 `CHANGES`，但系统不自动执行改写后的查询。

## 13. 测试与完成证明

### 13.1 语言与语义

- tokenizer/parser golden、源位置、错误恢复、深度/大小/fuzz；
- 查询级/MATCH 级作用域、默认稳定时间、冲突 scope；
- point/range/change、predecessor/successor、DELETE、回填；
- metadata provenance、需求驱动切分和 coalesce；
- `VALID FROM`、历史 system-time 写拒绝和 transaction overlay。

### 13.2 路径与执行

- fixed、variable、variable->fixed、fixed->variable、多 variable；
- point/range 精确区间、全路径 TRAIL、跨 shard frontier；
- path 与 fixed relationship 投影、property gather；
- morsel 确定性、低内存强制 spill、取消、文件清理；
- ColumnBatch 类型/null/offset/encoding round trip；
- `EXPLAIN ANALYZE` 完整指标。

### 13.3 分布式与后端

- PrimaryReplica 和 Shared-Nothing；
- leader 切换、Gateway/DataNode/Meta 重启、epoch fencing、deadline；
- 1PC/2PC、PREPARING/COMMITTED 边界、roll-forward；
- RocksDB/PostgreSQL/Neo4j point/range/change/path/write 差分；
- backend migration 后结果和 snapshot 等价；
- 现有 analytics takeover/ledger/checkpoint/GC 矩阵。

### 13.4 性能与资源

报告 parser/plan、point、range、change、1-hop、mixed path、join、aggregate、spill 和 exchange 的吞吐、p50/p95/p99、峰值内存、网络与后端时间。认证门槛沿用 DTGProxy 当前发布门槛：point 额外 p99 不高于直接 Adapter 15%，一跳不高于 20%，4 节点可分区吞吐不低于单节点 2.8 倍，8 节点不低于 5 倍。

完成必须有以下当前证据，不能以窄测试代替：

- workspace format、lint、unit、TCK、integration 全通过；
- 三后端 native mapping 全通过；
- 三后端 × 两部署模式迁移/接管矩阵全通过；
- clean-break 搜索门禁全通过；
- 性能基线产物已生成且满足门槛；
- Apache-2.0 NOTICE/来源记录完整；
- GitHub Actions 对最终提交全绿。

## 14. 实施顺序

1. 建立新 T-Cypher parser/AST/sema 与拒绝旧语法的 TCK。
2. 替换 Temporal IR，删除 Diff 和旧 scope。
3. 在表示无关的时态语义模块中实现并验证 interval derive、align、coalesce 纯函数及独立 oracle。
4. 在 compiler 和 Temporal IR 中完成 FactDemandSet、metadata provenance、coalesce 判等与物理算子契约，不依赖任何批次表示。
5. 在 Adapter 返回 bounded canonical primitive pages 之后，由 DTGProxy 边界 codec 转换为 `ColumnBatch`，再实现 point/range/change source、PropertyGather、MetadataProject 和 TemporalCoalesce 的向量化物理算子；不得要求后端提供列式布局，所有结果必须与表示无关 oracle 一致。
6. 实现 fixed/variable/mixed segmented frontier 与全局 TRAIL。
7. 完成 memory/spill/cancel/metrics。
8. 接入 distributed Exchange、Snapshot fencing 和事务。
9. 更新三后端 Adapter、分析投影和全部调用端。
10. 删除旧设计、旧代码、旧测试和旧文档残留。
11. 统一运行边界、故障、性能与 GitHub CI，修复全部失败。
12. 提交并推送 `feature/dtgproxy`。

## 15. 非目标

- 不迁移 Cedar 存储格式或 C++ runtime；
- 不维护旧 T-Cypher 语法兼容；
- 不支持无界变长路径；
- 不在本版定义 mixed-path `CHANGES`；
- 不把 scalar/materializing fallback 当生产实现；
- 不允许后端绕过 DTGProxy 修改受管理数据；
- 不因本次迁移重写已验证的 analytics ledger/GC 状态机。
