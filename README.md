# DTGProxy

DTGProxy 是面向时态属性图的分布式中间件。它提供 T-Cypher 语言入口、可验证的快照读写、Shard/Raft 复制、跨分片事务协调，以及在 Fjall、PostgreSQL 或 Kuzu 之一之上的统一图数据模型。

一次集群只选择一种官方后端：Fjall、PostgreSQL 或 Kuzu。Gateway 按 catalog 的围栏信息把分片请求路由到对应 Data 进程；同一集群的 active replica 不会混用 provider。

## 架构

```text
T-Cypher / Bolt client
          |
       Gateway
          |
   Data (one cluster-wide backend kind)
                  |
      Fjall / PostgreSQL / Kuzu (choose one)

Meta + Controller: catalog, cross-shard coordination, recovery, migration
```

- `dtg-language`：解析、语义校验和规范化逻辑 IR。
- `dtg-execution`：计划、快照隔离、Shard/Raft、查询执行、迁移和内置分析。
- `dtg-storage`：后端无关的逻辑副本契约；官方 provider 在 Data 进程内运行。
- 四个进程装配根：Gateway、Data、Meta、Controller；它们不复制业务算法。

## 后端与部署

一个集群由 `DTG_DATA_BACKEND_KIND` 固定为 `fjall`、`postgresql` 或 `kuzu` 之一；每个 Data 进程只能承载该类后端的分片。需要哪种后端就以该 kind 启动集群的 Data 进程，不能为了服务另一个分片再启用其他 provider。

生产 `DataProcessConfig` 也遵守同一约束：assignment 的 provider 必须与配置的 backend kind 一致；不匹配的 assignment 会在打开业务或共识命名空间前被拒绝。PostgreSQL 的 endpoint/credential profile 只会在 PostgreSQL Data 进程中加载。

开发启动器可按所选 backend 启动本地集群。PostgreSQL 可以在 loopback 上以随机凭据启动；Fjall 与 Kuzu 是嵌入式 provider，不需要另起数据库服务。

```bash
# 三选一；每次只启动所选后端的三个 Data 分片
scripts/local-cluster.sh start --backend fjall
scripts/local-cluster.sh start --backend kuzu
scripts/local-cluster.sh start --backend postgresql --managed-postgres

scripts/local-cluster.sh status
scripts/local-cluster.sh stop
```

`--managed-postgres` 与 `--postgres-url` 只适用于 PostgreSQL。该启动器仅管理它记录的 PID，
`stop` 会校验可执行文件；生产环境应改由 systemd、launchd 或 Kubernetes 管理进程和 PostgreSQL。

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

## 三后端端到端性能

下表是 2026-08-03 在开发机得到的真实四进程诊断结果，对应代码 revision
`5ec51fb`。每个 cell 启动独立的 Meta、Controller、Data 和 Gateway；Data 只绑定表中
的一种官方后端。Fjall 与 Kuzu 使用临时嵌入式目录，PostgreSQL 使用临时 loopback
PostgreSQL 实例，运行结束后已停止并清理。每个 cell 预热 1 秒、计量 5 秒，重复 3 次。

写入工作负载是单条 T-Cypher：

```cypher
CREATE (n:Bench {value: 1}) VALID FROM 1
```

每条请求经 Bolt → Gateway → Data → Shard/Raft → 官方后端原子 apply，只有收到
`COMMITTED` 才计入。计量后执行 `MATCH (n) RETURN COUNT(*)`；每个写 cell 都验证
`落盘顶点数 = 预热 CREATE 数 + 计量 CREATE 数`，所以表中的写 QPS 不包含 admission
回执或幂等重放。读数据集为 4,096 个落盘顶点；点查为 `n.id = 2048`，一跳和两跳分别
返回固定关系，计数为 `MATCH (n) RETURN COUNT(*)`。

**单顶点 `COMMITTED` 写入**（QPS；p50/p95/p99，ms）

| 后端 | 并发 | QPS | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fjall | 1 | 64 | 15.35 | 18.78 | 22.03 |
| Fjall | 8 | 262 | 30.05 | 42.26 | 44.60 |
| Fjall | 64 | 309 | 201.37 | 316.24 | 351.75 |
| Kuzu | 1 | 59 | 16.35 | 21.26 | 28.31 |
| Kuzu | 8 | 257 | 29.60 | 43.21 | 81.49 |
| Kuzu | 64 | 415 | 153.91 | 228.91 | 283.03 |
| PostgreSQL | 1 | 41 | 22.07 | 32.51 | 39.45 |
| PostgreSQL | 8 | 81 | 87.70 | 139.35 | 172.41 |
| PostgreSQL | 64 | 115 | 491.54 | 842.45 | 859.49 |

**完整 Bolt 读取**（QPS；p50/p95/p99，ms）

| 工作负载 / 并发 | Fjall | Kuzu | PostgreSQL |
| --- | --- | --- | --- |
| 点查 / 1 | 8,231; 0.122/0.150/0.221 | 9,121; 0.106/0.130/0.172 | 260; 3.606/4.962/5.871 |
| 点查 / 8 | 17,413; 0.441/0.664/0.900 | 21,501; 0.362/0.527/0.645 | 916; 7.060/19.518/24.344 |
| 点查 / 64 | 22,752; 2.748/4.040/4.756 | 26,334; 2.265/3.902/5.444 | 937; 56.784/145.070/173.563 |
| 一跳 / 1 | 7,755; 0.123/0.151/0.235 | 8,293; 0.113/0.159/0.226 | 285; 3.318/4.424/4.849 |
| 一跳 / 8 | 15,908; 0.480/0.728/0.969 | 14,445; 0.481/1.047/1.526 | 942; 7.283/16.144/20.733 |
| 一跳 / 64 | 20,778; 2.965/4.493/5.430 | 18,394; 2.777/7.827/12.146 | 917; 59.858/121.254/158.428 |
| 两跳 / 1 | 7,088; 0.134/0.170/0.276 | 6,467; 0.125/0.291/0.446 | 270; 3.746/4.395/4.769 |
| 两跳 / 8 | 14,528; 0.522/0.807/1.141 | 14,724; 0.450/1.093/1.980 | 712; 7.576/25.778/36.571 |
| 两跳 / 64 | 19,613; 3.196/4.670/5.438 | 19,115; 2.874/6.696/10.015 | 693; 52.915/212.578/255.165 |
| 计数 / 1 | 8,030; 0.119/0.146/0.235 | 8,155; 0.111/0.186/0.215 | 239; 3.826/6.927/8.279 |
| 计数 / 8 | 16,557; 0.456/0.692/1.019 | 19,812; 0.388/0.571/0.789 | 693; 7.452/31.475/48.192 |
| 计数 / 64 | 23,524; 2.631/4.015/4.907 | 29,295; 2.137/3.115/3.669 | 751; 52.646/200.257/234.655 |

读取单元格格式为 `QPS; p50/p95/p99`。这些是开发机端到端诊断，不是裸后端吞吐、生产
SLO 或跨机器横向扩展承诺。高并发单条强提交会显著增加排队和尾延迟；高吞吐写入应使用
批量摄入并轮询至 `COMMITTED`，而不是把单条提交写当作导入基准。

原始、版本化 artifact 位于（被 Git 忽略以避免提交大型延迟样本）：
`target/backend-e2e-committed-20260803-r2/fjall.json`、
`target/backend-e2e-committed-20260803-r2/kuzu.json`、
`target/backend-e2e-committed-20260803-r2/postgresql.json`。每份均包含 45 个原始 cell、
阶段指标、延迟样本、查询/结果摘要和逐写落盘审计。复现时需先构建 release 进程，并为
PostgreSQL 提供临时实例与凭据：

```bash
cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
DTG_BACKEND_E2E_SELECTED_BACKEND=fjall \
DTG_BACKEND_E2E_QUICK_REPETITIONS=3 \
DTG_BACKEND_E2E_QUICK_OUTPUT="$PWD/target/backend-e2e/fjall.json" \
cargo test --locked --release -p dtg-gateway --test backend_e2e_diagnostic \
  quick_selected_backend_e2e_comparison -- --ignored --exact --nocapture
```

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
