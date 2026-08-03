# DTGProxy

DTGProxy 是面向时态属性图的分布式中间件。它提供 T-Cypher 语言入口、可验证的快照读写、Shard/Raft 复制、跨分片事务协调，以及在 Fjall、PostgreSQL 和 Kuzu 三种官方后端之上的统一图数据模型。

项目的目标不是把三种数据库同时叠在一次请求下，而是让每个逻辑分片明确绑定一种底层数据库：KV 时态图分片使用 Fjall，关系型分片使用 PostgreSQL，图原生分片使用 Kuzu。Gateway 按 catalog 的围栏信息路由到对应 Data 进程。

## 架构

```text
T-Cypher / Bolt client
          |
       Gateway
          |
   Data (one backend kind per process)
    |           |            |
  Fjall     PostgreSQL      Kuzu

Meta + Controller: catalog, cross-shard coordination, recovery, migration
```

- `dtg-language`：解析、语义校验和规范化逻辑 IR。
- `dtg-execution`：计划、快照隔离、Shard/Raft、查询执行、迁移和内置分析。
- `dtg-storage`：后端无关的逻辑副本契约；官方 provider 在 Data 进程内运行。
- 四个进程装配根：Gateway、Data、Meta、Controller；它们不复制业务算法。

## 后端与部署

一个 Data 进程由 `DTG_DATA_BACKEND_KIND` 固定为 `fjall`、`postgresql` 或 `kuzu` 之一；该进程只能承载该类后端的分片。需要哪种后端就启动对应的 Data 进程，不需要为了服务某个 Fjall 分片而同时启动 PostgreSQL 或 Kuzu。

同机三后端示例会启动三个独立 Data 进程：各自拥有独立的业务、共识和后端命名空间。PostgreSQL 可以由开发启动器在 loopback 上以随机凭据启动；Fjall 与 Kuzu 是嵌入式 provider，不需要另起数据库服务。

```bash
scripts/local-cluster.sh start --managed-postgres
scripts/local-cluster.sh status
scripts/local-cluster.sh stop
```

该启动器仅管理它记录的 PID，`stop` 会校验可执行文件；生产环境应改由 systemd、launchd 或 Kubernetes 管理进程和 PostgreSQL。

## 一致性与写入

默认单分片 T-Cypher 写入走快照隔离 `COMMITTED`：Gateway 直接向目标 Data 发送带围栏、幂等标识的写命令；Data 分配提交时间、执行 Shard/Raft 与后端原子 apply 后才返回。该默认路径不依赖 Meta 的时间准备或提交决议。

跨分片显式事务继续由 Meta 的 `TemporalTxnCoordinator`、全局时间权威、2PC 和恢复路径处理。它们不会拖慢默认单分片写。

批量摄入使用 `DataService.AcceptSnapshotIngest`：每批最多 64 项、64 KiB，Data 仅在项目进入本地有界队列后返回 `PENDING`/`ACCEPTED` 回执。它不代表持久化或可读；客户端必须以同一 receipt ID 轮询 `GetSnapshotIngestReceipt` 或安全重试，直至 `COMMITTED` 或 `REJECTED`。进程崩溃可丢失尚未完成的内存回执，但同一 Shard command ID 的重试仍是幂等的。

## 性能解释

性能数字必须区分边界：

- Data ingress p99 的目标是 `≤ 50 µs`，仅指验证后的本机有界队列接收时间；它不包含 Gateway、Bolt、网络、Raft、fsync 或官方后端。
- `COMMITTED` 写延迟包含 Raft 和后端原子持久化，应独立报告。
- 读性能报告完整 Bolt → Gateway → Data → 后端路径的 QPS 与 p50/p95/p99；不把内存微基准外推成生产 SLO。

进程会输出版本化阶段指标。当前 schema 记录 `data_snapshot_ingest_admission` 与 `data_snapshot_ingest_receipt_lookup`，可用于分别量化摄入接收和回执查询。

2026-08-03 的开发机 Data admission 诊断（Fjall、单进程、请求预构造后计时、1,024 个样本）得到 p50 `17.084 µs`、p95 `20.084 µs`、p99 `37.667 µs`。可复现命令为：

```bash
cargo test --locked -p dtg-data diagnostic_snapshot_ingest_admission_latency -- --ignored --nocapture
```

这不是 `COMMITTED` 写入延迟，也不是完整网络路径或生产 SLO。

## 构建与验证

```bash
cargo fmt --all -- --check
cargo test --locked -p dtg-cluster-protocol
cargo test --locked -p dtg-data --test process
cargo test --locked -p dtg-execution --test gateway_process
bash scripts/check-layered-architecture.sh
bash scripts/tests/local-cluster-contract.sh
```

完整的开发机认证会写入版本化证据：

```bash
scripts/certify-clean-break.sh --local
```

## 文档

- [架构](docs/architecture.md)
- [部署](docs/deployment.md)
- [存储后端](docs/storage.md)
- [在线迁移](docs/migration.md)

`docs/superpowers/` 与 `docs/audit/` 中的记录用于追溯历史设计和实验，不构成当前运行说明。
