# DTGProxy Point History 与 Snapshot CSR Clean-Break 设计规范

日期：2026-07-28

状态：已批准架构方向，待书面规格复核

## 1. 决策

DTGProxy 采用两条互补且共享一致性快照的查询路径：

1. `PointHistoryReader` 负责单元素或小批量 `AS OF` 点查，直接在编码历史记录上按目标
   `valid_time` 定向重放，不再构造完整 `HistoryEntry` 链或反复复制完整
   `ProjectionRecord`。
2. `SnapshotCsr` 负责多跳、大扇出和重复图遍历。CSR 是由后端一致性读视图构建的、受内存
   预算约束的可丢弃执行缓存，不是持久化权威。

后端中的 `Current + History Anchor/Delta + Adjacency + applied_log_index` 继续作为唯一持久化
模型。这是新架构的权威存储契约，不是为旧实现保留的兼容格式。

本次迁移是 clean-break：新路径完成后删除旧的完整历史链解码、逐 Delta 全投影克隆重建、
无预算 CSR 构建、旧执行选择分支以及仅为旧 API 存在的包装函数。不得增加 feature flag、
双写、双读、旧符号别名或运行时回退来保留这些实现。

## 2. 目标与非目标

### 2.1 目标

- 降低最深 Anchor/Delta 链上点式 `AS OF` 查询的分配、复制和峰值内存。
- 为多跳和高扇出遍历提供连续、紧凑、快照一致的 CSR 执行格式。
- 保证 RocksDB、PostgreSQL、Neo4j 三种后端产生相同的时态和图遍历结果。
- 所有历史读取、CSR 构建、Overlay 和缓存发布都有显式条目与字节预算。
- 中间件重启或缓存淘汰不能影响图状态；所有执行缓存均可从后端重建。
- 用可复现基准确定执行路径阈值，并以数据而不是固定猜测决定生产默认值。

### 2.2 非目标

- 不用 CSR 替代后端持久化的 Anchor/Delta 或邻接索引。
- 不为每个可能的事务时间或有效时间永久保存一份 CSR。
- 不把 Rust 内存布局定义为 Adapter SPI、Sidecar 协议或磁盘格式。
- 不在本次改造中改变 T-Cypher 语义、Raft 提交语义或三后端持久化映射。
- 不保留旧查询执行路径用于兼容或对照；性能对照由独立基准提交和结果文件承担。

## 3. 总体架构

```text
T-Cypher query
    |
    v
Temporal/physical plan
    |
    v
Snapshot-bound path selector
    |
    +-- point or small-batch AS OF --> PointHistoryReader
    |                                  |
    |                                  +-- encoded history cursor
    |                                  +-- valid-time selective replay
    |                                  +-- demanded-property decode
    |
    +-- one-hop / low estimated fanout --> bounded backend adjacency primitive
    |
    +-- multi-hop / large or repeated traversal --> SnapshotCsrCache
                                                   |
                                                   +-- immutable Base CSR
                                                   +-- bounded Delta Overlay
                                                   +-- atomic rebuild/publish

All paths bind to one ReadSnapshot and one applied_log_index.
```

查询选择器只根据查询形状、估算展开量、缓存状态、内存 reservation 和已测得成本模型做决定。
后端类型不能改变查询语义，只能通过 capability 和实测成本影响物理路径。

## 4. PointHistoryReader

### 4.1 唯一接口

`temporal-storage` 提供新的快照绑定点查接口。具体 Rust 签名可在实施计划中按现有 lifetime
约束细化，但语义必须等价于：

```text
point_as_of(
    read_snapshot,
    element,
    transaction_time,
    valid_time,
    demanded_properties,
    byte_budget,
) -> Optional projected element
```

同时提供共享同一 `ReadSnapshot`、统一预算和 demanded-property 集合的小批量接口。批量接口
必须合并后端 point/range 请求，禁止退化为调用单点接口的 N+1 RPC 循环。

旧的 `load_history_chain_at -> Vec<HistoryEntry> -> reconstruct` 点查路径被删除。不得在新接口
内部重新收集完整链来模拟流式读取。

### 4.2 编码历史游标

新增 `HistoryCursor`，按后端历史键顺序读取不超过一个 Anchor 和十五个 Delta。游标必须：

- 固定在调用方提供的 `ReadSnapshot`；
- 校验所有页的 `applied_log_index` 一致；
- 在分配或保留记录前收取字节预算；
- 在看到 Anchor 后立即停止；
- 对缺少 Anchor、链过深、格式损坏和预算超限 fail closed；
- 允许 Adapter 返回有界页，但不得要求 Adapter 理解时态重放语义。

生产游标使用带 continuation、条目上限和字节上限的 canonical page primitive。现有一次返回
`Vec<KeyValue>` 的无分页 `scan` 不再作为生产历史读取接口；它只能保留在明确有界的测试
辅助代码中，或者随旧调用方一并删除。

链条条目数上限仍为十六。现有每 Anchor 最多十五个 Delta 和 64 KiB Delta 编码预算继续
生效。新路径还必须对单条 Anchor、单条 payload 和一次查询保留字节设置独立上限，避免
“Delta 有界但 Anchor 无界”。

### 4.3 零全量解码选择

新增借用编码字节的只读视图：

```text
HistoryEntryRef
ProjectionRecordRef
CanonicalElementRef
```

这些视图只解析固定头、区间、长度和 checksum，并可跳过未命中的 payload。只有满足以下
条件时才解码属性值：

- Anchor segment 包含目标 `valid_time`；或
- Delta 的 `changed_valid` 包含目标 `valid_time` 且操作为 Put；并且
- 属性属于 `demanded_properties`，或者调用方明确要求完整元素。

持久化编码仍由现有 canonical record contract 定义，但生产点查不得先构造
`BTreeMap<u32, GraphValue>` 再丢弃未请求属性。

### 4.4 定向重放

点查从 Anchor 在目标有效时间上的值开始，按事务提交顺序应用至目标事务时间的 Delta：

- 不包含目标有效时间的 Delta 只校验头和 checksum，然后跳过 payload；
- 包含目标有效时间的 Put 替换当前值；
- 包含目标有效时间的 Delete 清空当前值；
- 同一提交时间和 segment 顺序仍遵循现有 canonical key order；
- 最终结果与完整时态语义 oracle 严格相等。

点查路径不得调用旧 `rewrite_projection`、不得排序所有 segments、不得执行全投影
`coalesce`。

### 4.5 区间查询

需要返回完整有效时间区间的查询使用新的 `IntervalHistoryMaterializer`。它与点查共享
`HistoryCursor` 和编码视图，但显式构造有预算的区间结果。

旧 `rewrite.rs` 的逐 Delta 全量克隆算法被删除。新的 materializer 使用：

- 已排序的非重叠 segment buffer；
- 对编码 payload 的共享引用或 arena handle；
- 一次最终 coalesce，而不是每个 Delta 后完整排序和复制；
- 显式 segment 数、payload 字节和临时工作区预算。

## 5. Snapshot CSR

### 5.1 模块边界

新增独立 `snapshot-csr` crate，职责仅包括：

- 密集本地顶点 ID 字典；
- 不可变 CSR 构建和验证；
- 出边与入边方向的独立索引；
- Delta Overlay；
- 快照键、内存计量、缓存准入和重建策略；
- 有界遍历原语。

该 crate 依赖稳定的图 ID、时间和 Adapter 查询类型，但不得依赖 Parser、T-Cypher AST、
Bolt 或具体后端实现。`query-executor` 负责把物理 Expand 算子 lowering 到 CSR 或后端邻接
原语。

### 5.2 SnapshotCsrKey

每份 CSR 的身份至少包含：

```text
graph_id
shard_id
placement_epoch
applied_log_index
transaction_time predicate
valid_time predicate
direction
mapping/schema generation
```

精确点时刻 CSR 只包含该时态条件下可见的边，因此核心数组不重复保存完整双时态区间。
区间遍历如需保留多个可见 segment，使用单独的 versioned-edge 列式表，不得把点快照 CSR
伪装成区间语义。

### 5.3 内存布局

核心布局为结构分离数组：

```text
vertex_ids: Vec<u128>      local vertex id -> canonical vertex id
vertex_owners: Vec<u32>    local/boundary vertex id -> owner shard
offsets: Vec<u32/u64>      local vertex id -> adjacency range
neighbors: Vec<u32>        adjacency entry -> local destination/source id
edge_refs: Vec<u32>        adjacency entry -> edge column row
edge_ids: compact column
edge_types: Vec<u32>
```

只要单 Shard 邻接条目保证小于 `u32::MAX` 就使用 `u32` offset；超过时必须在构建前选择
`u64` 变体，禁止构建中途溢出。入边和出边按需独立构建，不得因一次单向查询自动分配双向
CSR。

原始 `u128` 顶点 ID 只在字典中保存一次。邻接数组使用 `u32` 本地 ID。所有数组在分配前
计算精确或保守 reservation；无法满足预算时返回可分类的 `InsufficientMemory`，由选择器
改走有界后端路径，而不是部分构建或进程 OOM。

字典同时包含本 Shard 顶点和遍历边界上出现的远端顶点。`vertex_owners` 保存 owner Shard，
使跨分片 frontier 能继续路由。不得因为压缩成 `u32` 而丢失 canonical `u128` ID、owner
Shard、edge owner 或 placement epoch。

### 5.4 构建与发布

构建器必须在一个真实 `ReadSnapshot` 内分页读取邻接和必要身份数据：

1. 获取并固定 `applied_log_index`；
2. 统计或有界估算顶点和邻接数量；
3. reservation 成功后构建本地 ID 字典；
4. 分页填充 degree、prefix sum 和邻接数组；
5. 验证 offsets 单调、终值等于邻接数、所有 local ID 有效；
6. 计算内容 fingerprint；
7. 完整成功后原子发布到缓存。

当前时刻 CSR 可以从当前邻接 primitive 构建。历史 CSR 不能只扫描当前邻接，因为已删除的
边可能在目标历史时刻可见。历史构建必须使用能够覆盖“当前不存在但历史可见”边的 canonical
edge identity/history candidate primitive，再通过批量 `PointHistoryReader` 按目标
transaction/valid time 过滤。不得通过当前邻接结果推断历史全集。

构建中的对象不可被查询看见。取消、deadline、snapshot fence 改变、placement epoch 改变、
Adapter generation 改变或任何校验失败都必须释放 reservation 和临时数组。

### 5.5 Delta Overlay

最新图的滚动缓存使用不可变 Base CSR 加有界 Overlay：

```text
added adjacency by local source
removed adjacency tombstones
overlay applied-index range
overlay retained bytes
```

Overlay 只接受连续、已提交且与 Base CSR 身份匹配的变更。缺失日志、乱序、epoch 变化或
mapping generation 变化立即使滚动缓存失效，不尝试猜测修复。

Overlay 订阅 durable apply 后的 canonical committed mutation。Base CSR 位于索引 `I` 时，
只有 Overlay 完整覆盖 `I + 1..=J` 才能服务索引 `J` 的查询。任何 gap 都禁止使用该缓存。
Overlay 更新失败不影响持久化提交，但必须立即失效对应缓存；查询不得看到“提交已成功但
CSR 尚未覆盖”的伪最新视图。

后台重建触发条件采用三者最早者：

- Overlay 边数达到 Base CSR 邻接数的可配置比例，初始候选值为 2%；
- Overlay 保留字节达到可配置绝对上限，初始候选值为 64 MiB；
- 基准或在线观测显示遍历成本相对纯 Base CSR 上升超过可配置比例，初始候选值为 10%。

这些初始值不是最终生产常量。正式默认值必须由三后端可复现实验确定并记录。

### 5.6 缓存策略

缓存使用显式节点级和查询级预算：

- 最新快照优先于历史快照；
- 历史快照按命中收益和 LRU 淘汰；
- 每个 Shard 默认最多保留一个最新版本和一个热点历史版本；
- 构建工作区与已发布缓存分别计量，防止重建时瞬时双倍内存失控；
- 被活跃查询引用的 CSR 不得强制释放，但新查询必须尊重剩余预算；
- 中间件重启后缓存为空，不能影响正确性或恢复过程。

## 6. 物理路径选择

新增唯一的 `GraphAccessPathSelector`，删除散落在执行器、worker 或 adapter 包装层中的旧路径
判断。选择器输入包括：

- point / interval / expand 查询形状；
- 深度和方向；
- 基数与 fanout 估算；
- demanded properties；
- 已固定的 snapshot identity；
- CSR 命中状态和构建成本；
- 查询 deadline、内存预算和取消状态；
- Adapter typed primitive capability 与实测成本模型。

初始逻辑仅作为基准种子：

```text
point/small-batch AS OF -> PointHistoryReader
one-hop or estimated expansion below threshold -> backend adjacency
depth >= 2, large expansion, or repeated snapshot -> Snapshot CSR when admitted
```

最终阈值从版本化 benchmark profile 加载。缺少可信 profile 时选择保守的有界后端路径，
不得为一次低收益查询盲目构建大型 CSR。

## 7. 一致性和错误语义

- PointHistoryReader、CSR 构建和后端邻接分页必须绑定同一个 `ReadSnapshot`。
- 所有返回页和缓存对象都携带精确 `applied_log_index`。
- 一个物理 fragment 内混用 CSR 和后端读取时，snapshot identity 必须完全相同。
- CSR miss、未准入和内存不足是可选路径结果，可以改走后端；数据损坏、snapshot mismatch、
  epoch mismatch 和非连续 Overlay 是正确性错误，必须 fail closed。
- 查询取消和 deadline 必须传播到历史游标、CSR builder、重建任务和遍历器。
- 不得把 PostgreSQL 全命名空间物化或 Neo4j 全图导出包装成“有界 CSR 构建页”。

## 8. Clean-Break 删除范围

新路径完成并通过门禁后，至少删除：

- 点查使用的 `load_history_chain_at -> Vec<HistoryEntry> -> reconstruct` 调用链；
- `history.rs` 中旧的完整链 `reconstruct`；
- `rewrite.rs` 中每个 Delta 都克隆、排序和 coalesce 全 Projection 的实现；
- 仅为这些函数存在的包装方法、错误分支和测试 fixture；
- 任何不带 snapshot identity 或内存 reservation 的 CSR/邻接缓存实验实现；
- 分散的旧 Expand 路径选择分支；
- PostgreSQL 通用扫描中为点查或 CSR 构建执行 `load_all_entries` 后内存过滤的生产路径；
- 旧性能基准入口和 feature flag。历史结果文件保留，旧可执行路径删除。

允许保留的内容只有被新架构直接使用的 canonical 持久化编码、Anchor/Delta 写入策略、
后端邻接索引和 correctness oracle。不得为了“以后可能回滚”保留 dead code。

## 9. 后端要求

### 9.1 RocksDB

- PointHistoryReader 使用原生 snapshot 上的有界 prefix/range iterator。
- CSR builder 使用 snapshot iterator 分页，不复制整个 column family。
- 记录 iterator bytes、decoded bytes 和 value-copy bytes。

### 9.2 PostgreSQL

- 使用 `REPEATABLE READ READ ONLY` 事务固定查询视图。
- 点查通过参数化 key/range SQL 读取目标历史链。
- CSR 通过有界、有序、可续传 SQL 页构建。
- 删除生产查询路径中的 `load_all_entries` 全命名空间物化。

### 9.3 Neo4j

- 使用一个显式 Query API transaction 固定视图。
- 点查和 CSR 构建使用有界参数化查询与 continuation。
- 不把逻辑导出接口用于普通查询或 CSR 构建。

三后端必须运行同一 fixed-seed temporal and traversal TCK。

## 10. 可观测性

每个查询至少记录：

```text
selected_access_path
snapshot_applied_log_index
history_records_scanned
history_bytes_scanned
history_payloads_decoded
history_payload_bytes_copied
csr_cache_hit/miss/admission_reject
csr_build_rows/bytes/duration
csr_retained_bytes
overlay_entries/bytes
backend_pages/rpc/wait
peak_query_retained_bytes
```

所有指标必须是有界计数器或直方图，标签中不得包含原始查询、元素 ID 或任意高基数字符串。

## 11. 测试策略

### 11.1 正确性

- 点查 fast path 与独立完整时态 oracle 对比全部有效时间边界。
- Anchor、1/15 个 Delta、新 Anchor、Put/Delete、空 projection、无限区间和同提交 segment 顺序。
- demanded property 与完整元素结果一致。
- CSR 与后端邻接路径对比一跳至多跳、入边/出边、环、自环、重复边和跨分片边。
- 历史 CSR 必须覆盖当前已删除但目标时间可见的边，并排除目标时间尚未创建的边。
- Base CSR + Overlay 与相同 `applied_log_index` 的后端结果一致。
- RocksDB、PostgreSQL、Neo4j 使用同一数据集和 fixed seed。

### 11.2 资源边界

- Anchor、payload、segment、history page、CSR build 和 Overlay 在 below/equal/above 边界测试。
- 构建取消、deadline、内存不足和失效后无 reservation 泄漏。
- 重建时旧 CSR 可用且进程峰值内存不超过预算模型。
- 任意损坏长度、checksum、offset、local ID 或 continuation fail closed。

### 11.3 Clean-Break 门禁

- 编译期确认旧公开符号不存在。
- 源码扫描确认生产路径不再调用旧 `reconstruct`、`rewrite_projection` 或 PostgreSQL
  `load_all_entries` 查询过滤路径。
- 不存在 legacy feature、compat module、dual-read 或 fallback-to-old-path。

## 12. 性能实验与验收

先在同一提交、同一固定数据集上记录旧实现隔离基线，再删除旧路径并测量新实现。基准必须
分别运行 Memory oracle、RocksDB、PostgreSQL 和 Neo4j，后端生命周期、CPU 并发、数据种子、
warmup、重复次数和统计方法固定。

### 12.1 Point AS OF 矩阵

- replay depth：0、1、8、15；
- valid segments：1、8、64、512；
- properties：小整数、混合小值、4 KiB、64 KiB；
- demanded properties：1、4、全部；
- snapshot age：最新、1k、100k commits。

验收条件：

- 所有场景结果与 oracle 一致；
- depth 15 的分配字节和 payload copy bytes 至少下降 60%；
- depth 15 的峰值 query retained memory 至少下降 50%；
- depth 8/15 的 p50 和 p95 均不得回退，并至少一项改善 30%；
- depth 0/1 的 p95 回退不得超过 5%。

### 12.2 Traversal 矩阵

- 图规模、degree distribution、深度 1/2/3/5；
- 冷 CSR、热 CSR、Overlay 0/1/2/5%；
- 单向与双向；
- 当前快照与重复历史快照；
- 单查询和并发查询。

验收条件：

- 热 CSR 在代表性 depth 2+ 场景的吞吐至少为有界后端路径的 2 倍；
- 热 CSR 的 p95 至少降低 30%；
- CSR 实际保留内存不超过预计算 reservation 的 115%；
- 冷查询在不具备复用收益时不会因盲目构建 CSR 而使 p95 回退超过 10%；
- Overlay 达到重建阈值前，遍历吞吐相对纯 Base CSR 下降不超过 10%。

未达到门槛时不得通过恢复旧执行路径解决。应优化新路径或调整经基准证明的选择阈值。

## 13. 交付顺序

1. 建立旧实现可复现基线和内存/复制指标。
2. 实现编码视图、HistoryCursor 和 PointHistoryReader，迁移所有点查调用方。
3. 实现 IntervalHistoryMaterializer，迁移区间调用方并删除旧 history/rewrite 路径。
4. 建立 `snapshot-csr` crate、构建器、验证器和内存 reservation。
5. 接入只读 Snapshot CSR 遍历和统一 GraphAccessPathSelector。
6. 接入最新图 Overlay、后台重建和 LRU/预算。
7. 为 RocksDB、PostgreSQL、Neo4j 实现真正有界的历史和 CSR 构建页。
8. 删除全部旧路径、compat/feature 和无界 PostgreSQL 查询路径。
9. 运行三后端 correctness、资源边界和性能矩阵。
10. 交付包含方法、数据、瓶颈、改动、删除范围、优化前后结果和结论的性能报告。

## 14. 完成定义

只有同时满足以下条件才算完成：

- 所有生产点查和区间查询只使用新 HistoryCursor/materializer；
- 所有多跳路径只由统一 selector 选择有界后端邻接或 Snapshot CSR；
- 旧历史重建、旧路径选择和兼容代码已删除；
- 三后端 fixed-seed TCK 和故障/边界测试通过；
- 性能门槛有真实、可复现数据支持；
- 中间件重启后缓存为空仍能从后端正确服务；
- 性能报告明确记录未解决瓶颈，不以 fixture 或模拟数据冒充真实后端结果。
