# DTGProxy 中间层读写性能收口设计

## 目标

在不改变 T-Cypher 语义、不绕过 Gateway/Data 边界、不把三种 provider 混入同一集群的前提下，
提升读写中间层的可解释性能，并使 Fjall、PostgreSQL、Kuzu 都有独立的三 Data shard 路由验收。

## 范围与非目标

本设计覆盖：完整 Bolt 读、单条 T-Cypher `COMMITTED` 写、显式批量 snapshot ingest 到
`COMMITTED` 的路径，以及三 backend 的静态 shard 路由验证。

不覆盖：迁移、重平衡、split/merge、follower read、跨机器部署优化或改变已有快照/提交语义。
单条 `COMMITTED` 写的目标不是几十微秒；它包含 Raft 与后端原子持久化，必须独立于 admission
延迟和批量提交吞吐报告。

## 性能合同

每个 benchmark cell 都通过真实 Bolt → Gateway → Data → provider 路径执行，报告 QPS、
p50/p95/p99、错误数、结果 digest 和阶段指标。每个正式 artifact 必须三次重复，并在空闲主机上
串行运行 backend；不得把单次开发机结果标为生产 SLO。

指标按边界分别解释：

| 边界 | 近期门槛 | 解释 |
| --- | --- | --- |
| Data snapshot-ingest admission | p99 ≤ 50 µs | 仅验证并进入有界队列，不代表落盘。 |
| Fjall/Kuzu 完整 Bolt 读 | 稳定 100k QPS 后再评估 200k | 使用 c64 点查、固定结果与固定数据集；必须同时报告 p99。 |
| PostgreSQL 完整 Bolt 读 | 以阶段剖析确定 adapter 优化前后的提升 | 不把嵌入式 provider 的吞吐要求直接套到 SQL adapter。 |
| 单条 `COMMITTED` 写 | 与批量写分开报告 | 包含一致性与持久化完成。 |
| 批量 snapshot ingest | 到 `COMMITTED` 的项目/秒和端到端尾延迟 | `PENDING` receipt 不计作写入完成。 |

## 设计

### 1. 阶段可观测性先行

保留版本化的 Gateway/Data request metrics，并在 backend diagnostic 中将一次请求归入以下互斥
阶段：Bolt 解码与编码、Gateway 编译与静态路由、Gateway–Data 等待/传输、Data fragment
校验与执行、provider 调用。指标以请求 ID 关联，只记录聚合时间和计数，不记录查询文本或凭据。

优化必须由某个阶段占比驱动；若瓶颈是 provider，则只修改对应 adapter，不在通用层引入
backend 特例。

### 2. 读取：复用有界 pipeline

不引入第二套 RPC。单 fragment、只读、非事务、未取消的请求使用现有 Gateway–Data pipelined
transport；多 fragment、写、显式事务和不支持 pipeline 的 endpoint 继续走已有 session/unary
回退。Bolt connection 的自动读 pipeline 保持严格 barrier：写、事务边界、失败和取消不得与其他
请求越过彼此。

在阶段证据显示通用层占主导时，按以下顺序优化：

1. 复用规范化逻辑计划、序列化 fragment 和固定 schema 的响应编码；缓存受 catalog/schema/fence
   约束，任何 fence 变化立即失效。
2. 在现有有界 batch/credit/response-batch 机制中调节批处理上限与执行并发，不等待填满 batch，
   不增加无界队列。
3. 对同机 Data 使用已有 Unix socket 选项；跨机器维持 TCP，不把本机 UDS 数字宣传为网络 SLO。

### 3. 写入：单条提交与批量提交分层

现有单条 Bolt `CREATE` 继续在收到 Raft 与 provider 原子 apply 后返回 `COMMITTED`，不得降级为
admission 成功。Data 内既有每 shard apply batcher 可以组提交并分别完成每个 command；阶段指标
必须记录排队、Raft apply 和 provider apply。

高吞吐写使用已有有界 `AcceptSnapshotIngest`/receipt 协议：每个 batch 受项目数、字节数和 receipt
容量限制，调用方以同一 receipt ID 轮询或重试至 `COMMITTED`/`REJECTED`。新的 E2E 测试只把
`COMMITTED` 项目计入吞吐，审计落盘计数并在取消、队列满、重复 receipt 和 provider 错误时保持
幂等语义。

### 4. 三 adapter 的独立多节点验收

抽取测试 topology builder，使 Fjall、PostgreSQL、Kuzu 都能建立三个同类、独立 namespace 的
Data endpoint。对每个 backend 断言：点查与邻接只访问静态 owner；scan/count fan-out 三个 endpoint；
每个 fragment 使用相同 catalog/schema/placement/transaction/valid-time fence；输出 rows 和
digest 一致。PostgreSQL 使用测试自管的 loopback 实例，Fjall/Kuzu 使用独立临时目录。

这验证的是多节点静态分片读取，不声称验证了 leader 故障切换或跨机器 HA。

## 失败与回退

Pipeline 仅在 endpoint 明确支持时启用；收到 `UNIMPLEMENTED`、流断开、credit 耗尽或请求取消时，
按已有类型化状态和 deadline 处理。仅 capability 缺失允许回退到 session/unary；已经提交到 pipeline
的请求不可在未知完成状态下重新发送不同 request ID。

任何 provider 的配置/assignment 不匹配必须在打开业务或共识 namespace 前被拒绝；后续实现将
把“拒绝 assignment 但 Data 进程 Ready”的状态改为启动失败，避免空承载节点被 supervisor 视作健康。

## 验收

1. 每个新增行为先有失败测试；读 pipeline、批量写、取消、错误隔离、fence 失效和 provider mismatch
都有确定性测试。
2. Fjall、PostgreSQL、Kuzu 的三 Data 静态路由测试全部通过。
3. 三 backend 的 release diagnostic 均生成三次重复 artifact，零错误、digest 一致并包含阶段指标。
4. 格式、差异、架构契约、协议/Data/Gateway 定向测试通过。
5. README 只发布经 artifact 支撑且明确边界的结果；未达标的性能如实保留为待优化项。
