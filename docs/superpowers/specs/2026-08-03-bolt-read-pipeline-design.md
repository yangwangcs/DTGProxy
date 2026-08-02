# Bolt 有界读 Pipeline 设计

日期：2026-08-03  
状态：已确认，待实施

## 目标

在不改变 Bolt 5.4 对客户端可见的消息顺序、结果内容、错误语义或事务语义的前提下，允许一个连接预先提交多个自动提交的只读 `RUN`/`PULL` 对。目标是消除“每条 Bolt 连接一次只能有一个完整往返”的并发上限；Gateway–Data pipeline、Data 端 credit 和 provider 继续沿用现有有界实现。

真实四进程 Fjall 基线表明，c64 point lookup 的 pipeline/session 吞吐分别为 37,573.7/37,574.9 QPS；Gateway 内部 RPC 约 0.86ms，Data provider 约 13µs。调高 Gateway worker 数仅带来约 2.8% 增益。因此本轮不调内部 batch/credit，而是处理 Bolt 入口的串行往返。

## 适用范围与不变量

- 仅允许已编译为 `GatewayOperation::Query` 的自动提交查询进入并发窗口；编译失败、写入、显式事务控制、analytics、`RESET`、`GOODBYE` 和未知消息均为串行屏障。
- 每个 Bolt 连接最多 64 个未终态读请求，且累计已接收未写出的请求消息不超过 64 KiB；超过任一限制时暂停读取，不丢弃、不无限积压。
- 每个读请求有独立 `GatewayCancellationToken`；连接断开或 `RESET` 取消尚未完成的读请求。一个请求的执行错误只产生该请求的 Bolt `FAILURE`，不得取消其相邻读请求。
- 对客户端可见的输出严格遵循输入请求对顺序：第 N 个 `RUN` 的 `SUCCESS/FAILURE` 和其对应 `PULL` 的 `RECORD*/SUCCESS` 在第 N+1 个请求的任何响应之前写出。
- 每个已完成但尚未轮到写出的结果计入每连接结果内存预算；超过预算时 reader 停止接收新的请求，直到 ordered writer 排空队首。
- 顺序客户端仍走同一状态机，并表现为与当前实现完全相同的 `RUN -> SUCCESS -> PULL -> RECORD*/SUCCESS` 序列。

## 架构

`serve_connection` 拆为三项协作职责：

1. reader 独占 socket read half，解码消息、为 `RUN` 创建有序 job，并记录关联 `PULL`。它只在读窗口和字节预算可用时读取下一消息。
2. executor 为已分类的只读 job 启动独立任务。任务只调用现有 `GatewayService::execute_statement`，并把 `GatewayResponse` 或 `BoltError` 发给有界 completion channel；它不直接写 socket。
3. ordered writer 独占 socket write half。它仅从队首 job 取终态：先写 `RUN` 的 fields `SUCCESS`（或 `FAILURE`），在收到该 job 的 `PULL` 后写 records 和最终 `SUCCESS`。写出完成才释放 request/byte/result-budget credit。

reader 与 writer 在 barrier 上协调：当 reader 看到非可并发消息时，停止接收后续消息，等待此前 read jobs 全部按序写出，再在单线程路径执行该消息；完成后再恢复 read window。`RESET` 取消当前窗口并等待其任务确认终态；`GOODBYE` 取消窗口后关闭连接。

为避免以字符串前缀猜测语义，`GatewayService` 增加一个只做编译与 `LogicalStatement` 分类的入口，返回 `Query` 或 barrier 类别。只读 job 随后仍走原有执行入口；该分类不会下推、不会缓存结果、不会改变 snapshot fence 或 planning context。

## 可观测性与基准

新增 Gateway detail：`bolt_read_pipeline_enqueue_wait`、`bolt_read_pipeline_execution_wait`、`bolt_read_pipeline_ordered_write_wait`，并把 schema 升级为 additive 版本。每个 detail 都按请求记录，且不得记录 statement、参数或结果内容。

基准增加能在一个 TCP Bolt socket 上连续写入多个 `RUN/PULL` 对、再按协议顺序读取响应的 `BoltPipelineSession`。保留既有 sequential session 作为对照；三次重复报告 c1/c8/c64/c256、TCP/UDS、pipeline depth 1/8/64 的 QPS、p50/p95/p99、错误数、结果 digest 和各阶段指标。

验收条件：所有结果 digest 与 sequential 基线相同、零错误、窗口/取消/barrier 测试通过；在 c64/c256 下，depth 8/64 的真实四进程 Bolt 吞吐相对 depth 1 有可解释的改善。200k+ QPS 是本机优化目标，不作为未测量时的承诺。

## 风险与回退

feature gate `DTG_GATEWAY_BOLT_READ_PIPELINE` 默认关闭。关闭时使用当前单 loop 实现。任一客户端协议违规、未知状态或预算耗尽都不改写已有结果；它们只暂停 reader 或走原有串行 failure/reset 行为。写入与事务绝不通过此优化路径。
