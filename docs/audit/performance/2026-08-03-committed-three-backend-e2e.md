# 三后端 `COMMITTED` 端到端诊断

日期：2026-08-03。代码 revision：`5ec51fb1ba6b500dc2743e8e6dd27b6682059403`。

## 边界与方法

每个 cell 独立启动 Meta、Controller、Data 和 Gateway。Data 由
`DTG_DATA_BACKEND_KIND` 固定为 Fjall、Kuzu 或 PostgreSQL；单个请求不会启动或访问另两种
业务后端。Fjall/Kuzu 使用临时目录；PostgreSQL 使用一次性 loopback PostgreSQL 实例，完成后
停止并删除。

写工作负载为 `CREATE (n:Bench {value: 1}) VALID FROM 1`。一次写只有在 Bolt 客户端收到
Gateway 传播的 `COMMITTED` 后计入；路径为 Bolt → Gateway → Data → Shard/Raft → 官方后端的
原子 apply。每个写 cell 在计量后执行 `MATCH (n) RETURN COUNT(*)`，并要求：

```text
persisted_operations == warmup_operations + operations
```

读数据集固定为 4,096 个已落盘顶点，点查使用 `n.id = 2048`；一跳、两跳关系均为固定单结果。
每 cell 预热 1 秒、计量 5 秒，三个重复。QPS 是三个 5 秒窗口聚合值；p50/p95/p99 从三个窗口的
原始样本按 nearest-rank 计算。

## 验收摘要

| 后端 | 原始 cell | 重复 | 错误 | 写落盘不变量违反 | artifact |
| --- | ---: | ---: | ---: | ---: | --- |
| Fjall | 45 | 3 | 0 | 0 | `target/backend-e2e-committed-20260803-r2/fjall.json` |
| Kuzu | 45 | 3 | 0 | 0 | `target/backend-e2e-committed-20260803-r2/kuzu.json` |
| PostgreSQL | 45 | 3 | 0 | 0 | `target/backend-e2e-committed-20260803-r2/postgresql.json` |

artifact 保留原始延迟样本、阶段指标、查询摘要、结果摘要、预热完成数、正式完成数和落盘完成数；它们
被 Git 忽略，因为单份文件包含完整直方图和样本，体积较大。README 汇总了其完整读写结果。

## 结论边界

结果是单台开发机、loopback 四进程诊断。它不衡量裸后端吞吐、跨机器复制、批量导入或生产 SLO。
`AcceptSnapshotIngest` 的 admission 微秒延迟与此处 `COMMITTED` 写入延迟是不同指标，不能互换。
