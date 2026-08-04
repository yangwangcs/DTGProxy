# Middleware 性能收口审计（2026-08-04）

本收口覆盖 provider mismatch fail-closed、三 provider 的三 Data 静态路由、Gateway/Data
阶段观测、有界 pipeline 选流、`COMMITTED` snapshot ingest，以及三后端 release E2E 诊断。

## 证据与边界

- 完整 release 读写矩阵和 SHA-256 见
  [release-backend-e2e.md](2026-08-04-release-backend-e2e.md)；每个后端 45 cell、三次重复、零错误。
- 单条写严格为 Bolt T-Cypher `CREATE` 到 Raft/provider `COMMITTED`，并用 `COUNT(*)` 审计
  `persisted = warmup + measured`；批量 ingest 严格计时到全部 receipt `COMMITTED`，并审计
  `persisted = committed`。
- 64 项批量 ingest（3 次重复）的 admission→`COMMITTED` 总吞吐为：Fjall 112.0、Kuzu 107.9、
  PostgreSQL 50.8 items/s。它不是单条 T-Cypher 写 QPS。
- 阶段均值由预热前的显式 Data/Gateway 基线与所有计量请求 drain 后的累积快照差得到；窗口可能
  含预热调用，只用于归因。worker drain 另有 5 秒上限，超时会使 cell 失败而非无限等待。

## 未达目标与已知限制

完整 Bolt 路径的 c64 点查尚未达到 100k QPS 目标；当前提交的本机 release 矩阵中，最高 c64
点查为 Kuzu 的 6.08k QPS。Data admission 微基准的 p99 37.667 µs 不代表完整 `COMMITTED` 写。结果均为单机、
串行 provider 诊断，不能外推为生产 SLO 或多机扩展结论。

`dtg-execution` 的既有 lib unit target 仍因未解析的 `ProcessWriteAccounting` 符号无法链接；
本收口的 integration target `dtg-execution --test gateway_process` 独立覆盖 Gateway 行为。
