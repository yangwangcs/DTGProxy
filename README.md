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

批量写的性能审计与单条写分开：批量计时从 `AcceptSnapshotIngest` admission 开始，直到本批所有 receipt 都达到 `COMMITTED`；只有此时才计入 `committed_operations`。诊断随后用本批最新提交时间建立不可变读快照，并通过 `MATCH (n) RETURN COUNT(*)` 核对 `persisted_operations == committed_operations`。因此 `PENDING`、未知回执、拒绝项或仅进入内存队列的项目都不会被算作落盘。

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

下表是 2026-08-04 在开发机得到的真实四进程 **release** 诊断结果。Fjall 与 Kuzu 的
artifact 对应 `a73f1ec`，PostgreSQL 对应随后仅修正阶段指标基线的 `fd03bf2`；该修正发生在
预热前，不改变 T-Cypher 请求执行、QPS 或延迟计量。每个 cell 启动独立的 Meta、Controller、Data 和 Gateway；Data 只绑定表中
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
| Fjall | 1 | 75 | 13.30 | 16.92 | 18.91 |
| Fjall | 8 | 258 | 29.92 | 50.80 | 65.24 |
| Fjall | 64 | 305 | 209.40 | 351.03 | 376.03 |
| Kuzu | 1 | 61 | 15.72 | 21.46 | 29.60 |
| Kuzu | 8 | 177 | 37.20 | 88.45 | 184.47 |
| Kuzu | 64 | 212 | 235.44 | 853.97 | 1,960.91 |
| PostgreSQL | 1 | 29 | 26.82 | 56.51 | 110.64 |
| PostgreSQL | 8 | 65 | 99.07 | 311.85 | 347.46 |
| PostgreSQL | 64 | 85 | 577.50 | 1,746.21 | 2,280.72 |

**完整 Bolt 读取**（QPS；p50/p95/p99，ms）

| 工作负载 / 并发 | Fjall | Kuzu | PostgreSQL |
| --- | --- | --- | --- |
| 点查 / 1 | 6,766; 0.136/0.193/0.283 | 2,522; 0.235/0.570/1.862 | 210; 4.049/9.246/10.936 |
| 点查 / 8 | 16,597; 0.443/0.851/1.194 | 8,820; 0.608/2.174/4.518 | 515; 11.531/43.242/70.427 |
| 点查 / 64 | 22,654; 2.617/4.845/5.925 | 12,082; 4.452/10.351/16.771 | 369; 143.307/349.122/396.674 |
| 一跳 / 1 | 7,236; 0.131/0.173/0.263 | 5,276; 0.144/0.328/0.643 | 140; 4.371/19.492/39.323 |
| 一跳 / 8 | 17,601; 0.434/0.749/1.001 | 13,887; 0.532/0.934/1.359 | 242; 20.032/108.039/155.912 |
| 一跳 / 64 | 22,604; 2.630/4.813/5.899 | 18,977; 3.154/5.167/6.983 | 284; 160.300/465.756/732.504 |
| 两跳 / 1 | 7,104; 0.134/0.179/0.282 | 7,060; 0.133/0.179/0.298 | 129; 7.243/11.866/16.788 |
| 两跳 / 8 | 14,664; 0.509/0.840/1.174 | 12,681; 0.508/1.294/2.252 | 197; 37.079/66.471/112.636 |
| 两跳 / 64 | 17,238; 3.552/5.648/7.410 | 15,158; 3.188/9.511/21.835 | 215; 290.197/407.078/500.320 |
| 计数 / 1 | 6,527; 0.143/0.202/0.316 | 5,684; 0.128/0.380/0.605 | 152; 6.368/8.536/9.364 |
| 计数 / 8 | 11,103; 0.537/1.654/3.502 | 10,053; 0.563/1.829/4.108 | 237; 31.725/55.735/75.806 |
| 计数 / 64 | 11,117; 3.880/15.307/29.701 | 12,140; 3.545/13.392/27.506 | 258; 266.327/367.905/408.621 |

读取单元格格式为 `QPS; p50/p95/p99`。artifact 中的阶段均值使用预热前基线到所有计量请求
drain 后快照的有界累计窗口，可能包含预热调用；它只用于路径归因，不作为单请求延迟。上述数值是开发机端到端诊断，不是裸后端吞吐、生产
SLO 或跨机器横向扩展承诺。高并发单条强提交会显著增加排队和尾延迟；高吞吐写入应使用
批量摄入并轮询至 `COMMITTED`，而不是把单条提交写当作导入基准。

批量 `COMMITTED` 诊断使用 64 项 snapshot batch，输出独立 artifact（不与 45-cell
T-Cypher 矩阵混合）：

```bash
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
DTG_BACKEND_E2E_SELECTED_BACKEND=fjall \
DTG_BACKEND_E2E_COMMITTED_INGEST_REPETITIONS=3 \
DTG_BACKEND_E2E_COMMITTED_INGEST_OUTPUT="$PWD/target/backend-e2e/fjall-committed-ingest.json" \
cargo test --locked --release -p dtg-gateway --test backend_e2e_diagnostic \
  committed_snapshot_ingest_selected_backend -- --ignored --exact --nocapture
```

将 `fjall` 替换为 `kuzu` 或 `postgresql` 即可复用同一方案；PostgreSQL 仍需提供临时
loopback endpoint/credential。artifact 中分别记录 admission→`COMMITTED` 的批量吞吐、
`committed_operations`、`persisted_operations`、错误数和三次重复汇总，不把批量结果冒充
单条强一致写入或 Data admission 微基准。

原始、版本化 artifact 位于（被 Git 忽略以避免提交大型延迟样本）：
`target/backend-e2e-release-20260804/fjall-r3.json`、
`target/backend-e2e-release-20260804/kuzu-r1.json`、
`target/backend-e2e-release-20260804/postgresql-r2.json`。每份均包含 45 个原始 cell、
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
