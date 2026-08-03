# 架构

DTGProxy 的依赖只允许单向流动：

```text
kernel <- language IR <- language
kernel <- storage contract <- provider implementations
language + storage <- execution capabilities <- execution facade <- process assemblies
```

`dtg-kernel` 只放标识、边界值、错误和确定性原语。`dtg-language` 只产生规范化逻辑 IR，不包含物理计划、RPC、事务或后端行为。`dtg-execution` 负责计划、快照隔离、Shard/Raft、受认证 follower read、逻辑副本快照、Snapshot CSR、内置分析、catalog、迁移与跨分片事务恢复。

## 后端绑定

一个逻辑副本绑定一个 provider class 与 backend generation。一个 Data 进程配置一个 `DTG_DATA_BACKEND_KIND`，因此只装配一种官方 provider：Fjall、PostgreSQL 或 Kuzu。该进程可承载同类的多个分片，但不能混合后端类型。

每个副本都有独立物理 namespace；一个 placement epoch 只有一个 active backend generation；同一 Raft group 的副本必须使用同一 backend class。Gateway 依据 catalog 中的 graph、shard、placement epoch 与 backend generation 围栏路由请求。

## 写入边界

默认单分片写是快照隔离的 1PC：Gateway 提交幂等 Shard command 到对应 Data，Data 分配提交时间并经 Raft 与 provider 原子 apply 完成后返回 `COMMITTED`。Meta 不在这条默认路径上。

显式跨分片写继续使用 Meta 协调的时间服务、2PC 和恢复。该边界确保普通单分片写不会为跨分片故障恢复的能力支付额外同步往返。

Data 批量摄入的 `ACCEPTED` 只是有界内存队列接收；`COMMITTED` 才表示持久化后可进入稳定可读快照。receipt ID 绑定请求指纹，重用同一 ID 提交不同载荷会被拒绝。
