# Gateway–Data 批量 / Pipelined API 设计

日期：2026-08-03  
状态：已实现（2026-08-03，`7e944ce`）

## 1. 目标与非目标

本设计只优化 Gateway 与 Data 之间的内部查询链路。对外 Bolt API、现有 unary `Execute`、现有 `ExecuteSession` 均保持兼容；跨主机继续支持 TCP，同机继续支持 UDS。

目标是减少每条请求的 gRPC ingress message 与调度边界，同时保持每条请求独立完成、独立返回和可观测。批量只合并入口，不建立“整批完成后才回包”的屏障。

本阶段不改变：

- Bolt `RUN/PULL` 顺序、事务与时态语义；
- Raft durability、snapshot fence、replica assignment；
- Data backend/provider 的读写实现；
- 写入主路径。写入继续使用现有单请求路径。

## 2. 现状与问题

当前 `GatewayService.ExecuteSession(stream GatewayRequest)` 已经使用 request ID 做 pending map，Data 端以 bounded `JoinSet` 并发执行，完成顺序可以与提交顺序不同。它的问题是每个请求都要单独编码、发送和调度一次；当前 32 的 session channel/permit 也同时承担了入口和执行上限。

此前的整批响应实现等待 `join_all` 后再发送，形成 head-of-line blocking，已验证会降低吞吐，因此不再采用。

当前 Gateway 的 `GatewayProtocolV2Client` 只传递 request 和 stage metrics，取消 token 尚未进入 Data session transport；新的接口必须补上这一边界。

## 3. 方案选择

### 方案 A：批量入口、逐条响应

在现有 session 上把若干 `GatewayRequest` 包进一个消息，Data 为每条 request 独立 spawn 并立即返回。改动小、容易灰度，但取消、信用流控和能力协商难以表达，收益主要局限于 ingress 编码。

### 方案 B：仅扩大现有 session 并发窗口

不改 protobuf，只提高 sender、pending 和 Data permit 上限。风险最低，但仍保留一请求一 ingress message，无法消除主要边界成本。

### 方案 C：新增全双工 `ExecutePipelined`（采用）

新增带 request batch、cancel 和 credit 的双向流。Gateway 通过短窗口聚合入口，Data 逐请求完成并乱序回包。协议可独立演进，既能保留旧 session 回退，也能把取消和背压定义为显式状态。

## 4. 协议

在 `dtg_cluster_v2.proto` 中新增：

```protobuf
message GatewayPipelineRequestBatch {
  repeated GatewayRequest requests = 1;
}

message GatewayPipelineCancel {
  RequestContext request = 1;
}

message GatewayPipelineClientFrame {
  oneof payload {
    GatewayPipelineRequestBatch batch = 1;
    GatewayPipelineCancel cancel = 2;
  }
}

message GatewayPipelineCredit {
  uint32 available_requests = 1;
}

message GatewayPipelineServerFrame {
  oneof payload {
    GatewaySessionResponse response = 1;
    GatewayPipelineCredit credit = 2;
  }
}

service GatewayService {
  rpc Execute(GatewayRequest) returns (stream GatewayResponse);
  rpc ExecuteSession(stream GatewayRequest) returns (stream GatewaySessionResponse);
  rpc ExecutePipelined(stream GatewayPipelineClientFrame)
      returns (stream GatewayPipelineServerFrame);
}
```

`GatewaySessionResponse` 继续代表一个 request 的完整结果，包含该 request 的所有 `GatewayResponse` column batches。因而一个 server frame 只归属于一个 request；同 batch 内各 request 的 server frame 可以任意顺序出现。

Data 必须验证：每个 request 有非零、16 字节 request ID；一个 batch 不超过 32 条且序列化大小不超过 64 KiB；每个 request 仍只允许一个 planned query fragment。违反单条约束的 request 返回该 request 的 typed error，不终止整条 stream。缺失 payload、重复 request ID、无法解析的 frame 属于 stream-level protocol error，可终止连接。

为使单条背压和取消可被客户端区分，`StatusCode` 增加 additive 值 `STATUS_CODE_RESOURCE_EXHAUSTED` 与 `STATUS_CODE_CANCELLED`；两者均不改变现有值的编号。前者使用 `RetryDisposition::Safe`，后者使用 `RetryDisposition::Never`。

协议采用 additive RPC。Gateway 连接时优先尝试 `ExecutePipelined`；服务端返回 `UNIMPLEMENTED` 或能力不匹配时，回退到现有 `ExecuteSession`，再回退到 unary `Execute`。旧路径的行为和指标保持不变。

## 5. Gateway 端数据流与背压

每个 Data endpoint 保持一个长期双向 pipeline stream。Gateway pipeline client 包含：

1. 有界 pending map，最多 256 个 request；
2. batch builder，限制 32 requests、64 KiB；只合并 writer 中已经就绪的请求，达到任一限制即 flush，绝不为凑批等待；
3. bounded outbound channel，不允许无界排队；
4. response reader，根据 request ID 从 pending map 取回 waiter；
5. credit counter，未获得 Data credit 时不再向网络提交新 request。

Data 初始发送 32 个 request credits；每条 request 进入终态（成功、typed error 或取消）后归还一个 credit。Gateway 的 256 条上限包括已排队和已提交请求，超过上限立即返回 `DTG-CLUSTER-PIPELINE-BACKPRESSURE`（`GatewayRetry::Safe`），而不是无限等待。批 builder 不设置填充等待窗口：低并发请求立即发送；只有本地 pending 容量或 Data credit 耗尽时，才在调用方 deadline 以内等待可用容量。

Data 端使用独立的 semaphore 限制执行并发（初始 32），输出 channel 有界。正常客户端遵守 credit；若客户端违反 credit 或 batch 限制，超限 request 获得 `ResourceExhausted` typed error，其他 request 继续执行。Data 读取输入时在无 permit/无输出容量时自然停止读取，从而把背压传回 Gateway，而不创建无界 task。

## 6. 取消与错误隔离

`GatewayProtocolV2Client` 增加带 `&GatewayCancellationToken` 的 pipeline 执行入口。Gateway 调用方取消时：

1. Gateway 将 request 从可交付 waiter 标记为 cancelling，并发送只含该 request ID 的 `cancel` frame；
2. 调用方立即得到 `DTG-EXECUTION-CANCELLED`，不会影响其他 pending request；
3. Data 为每个执行 task 保存独立 cancellation token/handle，收到 cancel 后只取消对应 task，并发送该 request 的 cancelled response；
4. Gateway 在收到终态或取消确认后释放 pending/credit；迟到或重复 response 只允许被丢弃并计数，不得使整个 session 失败。

传输级断开仍会失败所有未完成 request，并把 stream 标记为 inactive；这是连接级故障，不被伪装成单条业务错误。单条验证、backend error、deadline error 都编码为对应 request 的 `TypedStatus`。

## 7. 可观测性

保留既有 `gateway_query_session_submit` 与 `gateway_query_session_response_wait`，并已新增：

- `gateway_query_pipeline_submit`：从 Gateway 提交至 pipeline writer 的边界；
- `gateway_query_pipeline_response_wait`：等待及按 request ID 分发对应终态的边界。

batch bytes/count、credit、backpressure、cancelled 和 protocol error 由 Data 端计数。更细粒度的 enqueue、batch-build、flush、dispatch 和 cancel 子阶段留给下一轮剖析；在没有先证明其测量价值前，不以额外计时点干扰热路径。

Data 继续记录 `data_gateway_session_execution`，并为 pipeline 记录 batch request 数、bytes、credit、backpressure、cancelled、protocol error 计数。artifact 必须能区分 TCP/UDS、unary/session/pipeline 和每个 concurrency。

## 8. 实施分层

实现按以下边界拆分：

- `dtg-cluster-protocol`：只增加 protobuf 类型和 additive RPC；
- `dtg-execution::gateway`：pipeline client、batch builder、pending/credit/cancel 状态机，以及旧路径回退；
- `dtg-data::service`：pipeline stream handler、单条 task、permit、cancel map 和 typed error；
- `dtg-gateway`：只切换内部 transport capability，不改 Bolt handler；
- 测试支持模块：新增 pipeline transport 开关、artifact 字段和基准矩阵。

## 9. 测试方案与验收

### 协议/单元测试

- batch 的 request 数、bytes、零 ID、重复 ID 校验；
- 乱序完成仍按 request ID 正确归属；
- 一个 request 出错不影响同 batch 其他 request；
- 取消 request A 不取消 request B；重复/迟到 response 不会关闭 session；
- queue、credit、batch request、batch bytes 四种背压边界；
- `ExecutePipelined` 不可用时依次回退 session、unary。

### 进程/E2E 测试

使用现有四进程真实 Bolt/Data 路径，读 workload 为 point lookup、one-hop、two-hop、count vertices；每个 workload 在 TCP 与 UDS 下比较：

- unary/session baseline；
- pipelined API；
- concurrency 1、8、64；
- 至少 3 次重复，预热与测量分离；
- QPS、p50/p95/p99、错误数、结果 digest；
- Gateway/Data stage metrics，重点检查 response wait、batch build、dispatch、Data execution。

验收条件：零错误、结果 digest 与 baseline 一致、无整批等待证据（较慢 request 不阻塞较快 request）、取消和背压测试通过；pipeline 在 c8/c64 的正式结果必须相对 session baseline 提供可解释的吞吐改善，否则保留实现但明确记录瓶颈与下一轮优化项。100k+ QPS 是优化目标，不作为未测量时的承诺。

## 10. 风险与回退

风险包括 protobuf 能力不一致、credit 状态漂移、取消时的迟到 response，以及 batch 聚合窗口在低并发下增加尾延迟。所有风险均可通过 feature gate `DTG_GATEWAY_QUERY_PIPELINE` 控制；关闭或协商失败时继续使用当前经过验证的 UDS/TCP session/unary 路径。任何性能退化不得替换现有默认路径，除非同一基准矩阵证明收益。
