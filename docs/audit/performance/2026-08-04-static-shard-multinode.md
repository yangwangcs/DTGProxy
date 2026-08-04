# 静态分片多节点 snapshot 查询验收

日期：2026-08-04。验收提交：`ef97e90b74ff4f0f0525edee8551a21ccd53110a`；
release process binaries 构建时的 Git revision：
`085195f7205eaad4be7d942db01defb9ecf6391b`。验收提交只新增测试、fixture/assertion 修正和本审计，
未改动 release process runtime。

## 路由验收边界

`static_shard_snapshot_routes_point_adjacency_and_count_to_three_data_endpoints` 启动三套
独立 Fjall `DataNode`，每套只装载一个静态 active shard（13、14、15），并通过生产
`GatewayProtocolV2Transport` 与 `ShardRoutedGatewayTransport` 的同一请求/响应协议接入一个
Gateway service。测试 transport 只把生产 Data RPC service 保留在进程内，未伪造路由响应，也未
绕过 Data 的 immutable read view。

测试 workload 与验收结果：

| 查询 | 预期 owner/fan-out | 实际 endpoint calls | 结果 |
| --- | ---: | ---: | --- |
| `MATCH (n) WHERE n.id = $id RETURN n.id`（id=41） | shard 15 / 1 | `[0, 0, 1]` | 通过，返回 id=41 |
| `MATCH (a)-[r]->(b) WHERE a.id = $id RETURN r`（id=41） | shard 15 / 1 | `[0, 0, 1]` | 通过，返回 1 条邻接边 |
| `MATCH (n) RETURN COUNT(*)` | shards 13,14,15 / 3 | `[1, 1, 1]` | 通过，合并计数=4 |

五个成功 fragment round-trip 的 outbound request fence 均保持：`catalog_revision=29`、`schema_version=31`、
`placement_epoch=17`、`backend_generation=23`、同一 capability digest、各自 catalog
applied index、`transaction_time=41`、`valid_at=10` 和 `snapshot_immutable=true`。
生产 response 不回显 `ReadFence`；本验收证明的是 Data 仅在这些固定 fence 下接受并成功执行请求，
不声称 row payload 自带 fence metadata。

验证命令：

```text
cargo test --locked -p dtg-gateway --test four_process_cluster static_shard_snapshot_routes_point_adjacency_and_count_to_three_data_endpoints
```

结果：通过（1 passed，3.82s）。TDD Red 阶段先移除 shard route map，点查按生产协议返回
`replica is not hosted on this node`；补上 14→第二节点、15→第三节点的现有静态 route map
后转绿。

## 选定 backend diagnostic

三次诊断均使用当前 revision 构建的 release process binaries：

```text
cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR=$PWD/target/release
```

每次 quick diagnostic 使用 1 个 disposable provider cell、5 个 workload
（`create_vertex`、`point_lookup`、`one_hop_expand`、`two_hop_expand`、`count_vertices`）和
3 个并发级别（1、8、64），共 15 cells。`result_digest` 是诊断对实际 Bolt fields/rows 计算的
SHA-256；同一 workload 的三个并发 cell 必须产生相同 digest。完整运行命令为：

```text
DTG_BACKEND_E2E_BIN_DIR=$PWD/target/release \
DTG_BACKEND_E2E_SELECTED_BACKEND=<fjall|postgresql|kuzu> \
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic \
  quick_selected_backend_e2e_comparison -- --ignored --nocapture
```

| backend | 环境/命令 | cells | errors | 运行时间 | result digests | 结论 |
| --- | --- | ---: | ---: | ---: | --- | --- |
| Fjall | `DTG_BACKEND_E2E_SELECTED_BACKEND=fjall` | 15 | 0 | 156.96s | 见下表，三个并发级别一致 | 通过 |
| Kuzu | `DTG_BACKEND_E2E_SELECTED_BACKEND=kuzu` | 15 | 0 | 137.05s | 见下表，三个并发级别一致 | 通过 |
| PostgreSQL | `DTG_BACKEND_E2E_SELECTED_BACKEND=postgresql`，`DTG_BACKEND_E2E_POSTGRES_ENDPOINT=host=127.0.0.1 port=5432 dbname=dtg connect_timeout=1`，`DTG_BACKEND_E2E_POSTGRES_CREDENTIAL=user=dtg password=secret` | 0 | 1 | 1.28s | 无结果 digest | 未通过：本机 `127.0.0.1:5432` 无 PostgreSQL 服务，Data replica 未 hosted |

Fjall 与 Kuzu 的实际 Bolt result digests 相同；每个值都在 concurrency 1、8、64 三个 cell 中
重复得到：

| workload | `RawObservation.result_digest` |
| --- | --- |
| `create_vertex` | `374708fff7719dd5979ec875d56cd2286f6d3cf7ec317a3b25632aab28ec37bb` |
| `point_lookup` | `a20835b347e488e5c5c577fc72ededaf27d1d7d180564a9c9e23d73d895f2f2f` |
| `one_hop_expand` | `474ac19cfc5bbd1eebe434723ca0c2f9823f233a81811864cb5ba125d4cc01b1` |
| `two_hop_expand` | `0243b831c62abf2b92d28a902ddddd2f585d4c59cff0a7d8c892f17ba06ee012` |
| `count_vertices` | `472d2580d4b07a844e8441cf554cac7e166ebd475f490b72a1dbe672b738ea7f` |

PostgreSQL 结果明确保留为失败；它不代表 PostgreSQL adapter 通过，也不把 unavailable live
service 计入成功。以上 quick diagnostic 是现有单 Data endpoint 的 backend smoke/性能诊断，
多节点静态 shard 路由的完整 endpoint-call 验收由前一节的三 DataNode snapshot 测试负责。

## 不在本任务范围

没有实现迁移、重平衡、split/merge、follower read 或第二套 routing/RPC protocol；没有将
单机 loopback 诊断解释为跨机器生产 SLO。
