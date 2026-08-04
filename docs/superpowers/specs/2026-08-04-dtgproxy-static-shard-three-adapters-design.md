# DTGProxy 静态分片与三后端独立转译设计

状态：待用户审核

日期：2026-08-04

## 1. 目标

在不实现在线迁移、自动重平衡和 follower read 的第一阶段，完成一个可验证的多节点
分布式时态图闭环：

- 一个集群运行时只绑定一种后端；
- 转译层分别提供 Fjall、PostgreSQL、Kuzu 三个独立 adapter；
- T-Cypher 经过语言层标准化后，由执行层完成静态分片路由、快照围栏和多节点查询；
- 三个 adapter 共享逻辑存储契约，但不共享后端物理实现或物理文件格式。

第一阶段的性能优化目标是减少跨分片往返和交换数据量，而不是先实现动态拓扑优化。

## 2. 部署不变量

`DTG_BACKEND_KIND` 在集群启动配置中固定为 `fjall`、`postgresql` 或 `kuzu` 之一。
Meta catalog、Data assignment 和每个 Raft group 都必须验证该 backend class 一致。

三种 adapter 都编译进发行版，但非当前选择的 adapter 不会被打开，也不会启动对应的
外部服务。Fjall 和 Kuzu 使用嵌入式目录；PostgreSQL 由每个 Data 节点连接其配置的
PostgreSQL 实例。

## 3. 分层职责

### 3.1 语言层

语言层负责词法、语法、语义检查、时间范围解析和规范化逻辑 IR。IR 只描述 scan、point
lookup、expand、filter、projection、aggregate、order、limit 和 write，不包含 shard、
Raft、provider 或 RPC 类型。

### 3.2 执行层

执行层新增或收敛以下组件：

- `ShardRouter`：使用固定 hash 规则将 vertex/edge owner 映射到 shard；
- `SnapshotCoordinator`：为一次查询固定 catalog revision、transaction time、placement
  epoch、backend generation 和每个 shard 的 applied index；
- `FragmentPlanner`：将逻辑 IR 拆为带 read fence 的 shard-local fragment；
- `DistributedQueryExecutor`：并行发送 fragment，执行 shard-local partial aggregate，
  再做确定性 merge；
- `FrontierBatcher`：多跳遍历按目标 owner shard 分组，以有界批次推进 frontier。

第一阶段只读 Leader。任何 epoch、generation、leader term 或 applied-index 围栏失效，
查询整体安全重试，不返回混合快照。

### 3.3 转译层

转译层定义后端无关的有界逻辑操作：

- `PointRead`、`BoundedScan`、`AdjacencyRead`、`TemporalHistoryRead`；
- `ApplyCommittedBatch`；
- `OpenReadView(ReadFence)`；
- `ExportLogicalPage`/`ImportLogicalPage` 仅作为后续迁移接口，不进入第一阶段运行路径。

三个 adapter 独立实现：

| Adapter | 物理实现 | 第一阶段职责 |
|---|---|---|
| Fjall | 有序 keyspace、版本记录和邻接记录 | 首个完整多节点验收后端 |
| PostgreSQL | 时态表、索引、事务 read view | 独立 adapter 契约与端到端单后端验收 |
| Kuzu | 原生图数据库 namespace 与时态辅助记录 | 独立 adapter 契约与端到端单后端验收 |

adapter 只能返回逻辑 page 和能力/保证声明。它不能决定分片路由、全局快照或事务冲突。
执行层对 `Candidate` 结果保留 residual temporal filter；只有明确声明 `Exact` 才可
省略对应 residual。

## 4. 快照读取协议

1. Gateway 根据 catalog 创建 `SnapshotRequirements`。
2. 对本次涉及的每个 shard，向 Leader 获取 read permit，确认同一 transaction time 与
   applied index。
3. 生成每个 fragment 的 `ReadFence`，包含 binding、placement epoch、backend generation、
   capability digest 和 applied index。
4. Data 在 provider 上打开不可变 read view，执行 fragment 并返回有界 page。
5. Gateway 校验所有 fragment fence 与 snapshot identity 一致后合并结果。

点查和一跳遍历只访问 owner shard；scan/count/全局聚合 fan-out 到多个 shard，并优先在
每个 shard 做 partial aggregate。

## 5. 测试与性能验收

每种后端分别执行，不同时启动三类业务后端：

- 1、3、8 Data 节点拓扑；
- 固定静态 shard 数与复制因子；
- 点查、一跳、两跳、count、时间点读取；
- Leader 故障后重新选主并重复同一快照查询；
- 三个 adapter 使用相同逻辑数据集、查询、结果 digest 和错误语义。

性能报告必须分开记录：

- 语言解析/规范化耗时；
- 执行层路由与快照协调耗时；
- RPC/交换耗时；
- adapter 本地执行耗时；
- 完整 Bolt → Gateway → Data → adapter 的 p50/p95/p99 和 QPS。

不把单进程 admission 微基准或单条 `COMMITTED` 写入结果宣传为多节点吞吐。

## 6. 明确延期

第一阶段不实现：在线后端迁移、split/merge、自动重平衡、热点迁移、follower snapshot
read、跨后端联合查询、既有外部 schema 自动发现和 CDC 回填。

这些能力必须建立在本设计的静态分片、快照围栏和三个 adapter 契约之上。
