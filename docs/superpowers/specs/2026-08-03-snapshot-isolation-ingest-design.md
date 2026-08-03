# 快照隔离写入与批量摄入设计

日期：2026-08-03
状态：已确认，实施中

## 目标

默认单分片 T-Cypher 写入使用快照隔离并只在真实提交后返回成功；删除该路径对 Meta 的同步时间准备和提交决议。新增 Data 层的批量摄入入口，提供有界、可重试的 `ACCEPTED` 回执，目标仅针对 Data 进程内的入队时间 p99 不超过 50 微秒。

## 语义

- `COMMITTED` 是默认 T-Cypher `CREATE`/`UPDATE` 的语义：Data 已完成 SI 冲突验证、Shard/Raft 提交、官方后端原子持久化，并推进该 Shard 的可读快照水位。普通读只能看到该水位以内的数据。
- 默认单分片写不再调用 Meta 的 prepare/resolve RPC。Gateway 从当前 catalog 取得 immutable snapshot fence，生成幂等 transaction/command identity，并将写提交给目标 Data；Data 的每-Shard 有界 batcher 将连续命令交给既有 Raft ready 批和后端原子 apply。
- 显式跨分片事务保留 `TemporalTxnCoordinator`、时间权威和 2PC/recovery；它们不再是普通单分片 T-Cypher 写的依赖。
- `ACCEPTED` 仅表示 Data 已将带幂等 receipt 的摄入项放进进程内有界队列。它不表示持久化、可读或崩溃可恢复。客户端必须查询 receipt，或以相同 receipt id 重试。
- receipt 状态为 `PENDING`、`COMMITTED` 或 `REJECTED`。Data 崩溃会遗失尚未完成的 `PENDING` receipt；重试仍安全。队列满返回 `RESOURCE_EXHAUSTED`/safe retry，不排队等待。

## API 与资源边界

- `DataService.AcceptSnapshotIngest` 接收最多 64 项、总序列化载荷最多 64 KiB 的批次；每项是一个受现有 Shard fence 和 payload 校验约束的单分片写命令，且 receipt id 非零。
- Data 进程最多保留 4,096 个 pending/recent receipt；完整时拒绝新 receipt，不驱逐 pending 项。完成 receipt 按有界 FIFO 淘汰；淘汰后重试会重新接收，使用同一 Shard command id 的状态机幂等性防重。
- `DataService.GetSnapshotIngestReceipt` 只查询本进程内 receipt 表；未知 receipt 返回 `NOT_FOUND` 语义的安全重试响应。
- Data 接收路径不等待 Raft、后端或 fsync，因此性能基准只计量 request 已完成验证并进入有界通道的时间；提交延迟单独报告。

## 兼容与清理

- `ApplyTransaction` 保持给显式事务、Raft/recovery 和默认 `COMMITTED` 单写使用。
- 删除 Gateway 默认写入中的 Meta prepare/resolve、旧的进程内 write-accounting 状态机及其指标；保留 Meta API，供跨分片协调与恢复使用。
- 文档只把 Data 内存 `ACCEPTED` 的 p99 作为微秒级目标；不将其混同于 Bolt、Gateway、网络或稳定存储确认。

## 验证

- Gateway 测试证明普通 `CREATE` 仅调用 Data commit，不调用 Meta。
- Data 测试证明 SI 写在完成后才成为可读快照；ingest 批次按顺序返回 receipt、幂等重投不重复入队、满队列安全拒绝，且 receipt 从 pending 转为 committed/rejected。
- Fjall、PostgreSQL、Kuzu 均复用 state-store atomic batch contract；Shard determinism 和 storage semantics 覆盖连续 batch、冲突和 replay。
