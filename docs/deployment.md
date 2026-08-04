# 部署

DTGProxy 由 `dtgproxy-gateway`、`dtgproxy-data`、`dtgproxy-meta` 和 `dtgproxy-controller` 构成。生产部署应使用平台 supervisor 管理进程、证书、数据卷与 PostgreSQL；本地启动器仅用于开发和集成验证。

## Data 后端选择

每个 Data 进程必须设置 `DTG_DATA_BACKEND_KIND`：

```text
fjall | postgresql | kuzu
```

其 `DTG_DATA_ASSIGNMENTS` 中的所有分片必须匹配这个值。一个集群的 active Data 副本
只能使用同一种后端；若需运行其他后端，应部署为另一套独立集群：

```text
data-fjall       DTG_DATA_BACKEND_KIND=fjall
data-postgresql  DTG_DATA_BACKEND_KIND=postgresql
data-kuzu        DTG_DATA_BACKEND_KIND=kuzu
```

Gateway 只连接当前请求目标分片的 Data endpoint；一个 Fjall 请求不要求 PostgreSQL 或 Kuzu 进程存活。Fjall 与 Kuzu 使用进程内存储目录；PostgreSQL 需要 `DTG_DATA_POSTGRES_ENDPOINT` 与 `DTG_DATA_POSTGRES_CREDENTIAL`。

每个 Data 副本必须使用独立的业务目录、共识目录与后端 namespace。Data 会拒绝 provider 与声明后端不匹配的 assignment。

## 本地安全启动

```bash
# Fjall（默认值；显式指定便于脚本化）
scripts/local-cluster.sh start --backend fjall

# Kuzu
scripts/local-cluster.sh start --backend kuzu

# PostgreSQL：启动器管理一个临时的、仅 loopback 可达的实例
scripts/local-cluster.sh start --backend postgresql --managed-postgres

# 或连接用户自行管理的 PostgreSQL
scripts/local-cluster.sh start --backend postgresql --postgres-url 'host=127.0.0.1 port=5432 dbname=dtgproxy user=dtgproxy sslmode=disable'

scripts/local-cluster.sh status
scripts/local-cluster.sh stop
```

`--managed-postgres` 与 `--postgres-url` 只可同 `--backend postgresql` 使用。启动器只监听
loopback，创建私有运行目录和随机 PostgreSQL 凭据，并只记录、停止它自己启动且可执行文件
匹配的进程。不要把开发启动器用于生产。

## 网络与安全

跨主机使用 mTLS。明文监听只允许 loopback。Gateway 与同机 Data 可以配置 owner-only Unix socket；跨主机仍使用 TCP。Meta 与 Controller 是 catalog、跨分片协调、恢复和迁移控制面的组件，不是默认单分片写的同步依赖。

## 写入与摄入运行语义

默认单分片 T-Cypher 写在 Data 完成 Raft 与后端原子提交后返回 `COMMITTED`。批量摄入 `AcceptSnapshotIngest` 的初始 `PENDING` 回执仅表示内存队列接收，必须轮询 receipt 或使用相同 ID 重试。未知或未完成回执应安全重试；队列满返回 `RESOURCE_EXHAUSTED`。
