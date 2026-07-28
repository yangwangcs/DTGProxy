# DTGProxy 后端无关系统性能内核设计

日期：2026-07-24

状态：已批准，作为 `2026-07-23-dtgproxy-cedar-tcypher-clean-break-design.md` 的性能扩展。

## 1. 目标

DTGProxy 必须保留一套后端无关的 T-Cypher 语言、时态语义、快照和 Adapter SPI，同时系统性降低中间件相对于直连后端增加的固定与放大开销。优化范围同时覆盖：

- 点查和短路径的 p50/p99 延迟；
- 跨分片扫描、分析和变化查询的吞吐；
- 协调器、worker 和 Adapter 边界的峰值内存；
- 额外网络字节、序列化次数、值复制次数和 RPC 次数。

性能优化不得改变 T-Cypher 可见语义、SnapshotToken fencing、capability fail-closed 规则、资源预算或后端独立性。

## 2. 不变量

`ColumnBatch` 是 DTGProxy 查询执行器的内部 ABI，不是语言层类型、Adapter SPI 返回类型，也不是任何后端必须采用的物理布局。所有优化都以如下边界为准：

```text
T-Cypher / AST / IR / Physical DAG
        -> typed bounded primitives -> canonical pages
        -> Adapter boundary codec -> adaptive local execution
        -> ColumnBatch only where vector execution or exchange needs it
```

Adapter 继续只返回带有 bounds、snapshot/applied-index 和 capability guarantee 的 canonical page。不得下推原始 T-Cypher；不得把 ColumnBatch、Rust 内存布局或后端私有对象加入 SPI。

`Candidate` guarantee 的任何结果都必须经过 DTGProxy residual filter。第一阶段即使 `Exact` 也不删除 residual；先证明计划、能力 generation、`valid_time` 和运行时 page guarantee 的一致性，再考虑后续等价优化。

## 3. 推荐架构：自适应旁路执行内核

同一 physical DAG 使用同一种 slot/schema 和语义，但在 lowering 时按访问形态选择低开销路径：

1. 点查和短路径走批量 typed primitive、路由合并与小页直接消费路径。只在表达式或网络边界确有需要时构造 `ColumnBatch`。
2. 大扫描、聚合和分析走 morsel 化 columnar pipeline。Filter、Project 和可融合的 Expand 在同一批内执行；PropertyGather 必须在候选、时态域和 residual 剪枝之后运行。
3. 跨分片结果以 credit 控制的 canonical exchange morsel 逐批传输。worker 不得先收集整个结果，再编码成 `Vec<WorkerBatch>`；coordinator 不得以总结果大小作为缓冲成本。
4. Adapter page 与 exchange frame 允许引用或移动已有 buffer；长度预检、编码和解码不得为了查看值而复制大 string、bytes、list、map 或图元素。

该策略是自适应执行，而不是按后端类别分支。后端 capability 只决定某个 typed primitive 是否可用及其 guarantee，不能改变语言语义或执行结果。

## 4. 首批实施切片

### 4.1 Exchange 借用访问与单次编码

为 `ColumnBatch` 增加内部借用式访问接口，使 exchange codec 的 exact-size prepass 和实际写入都从同一列 buffer 读取。移除当前 `ColumnBatch::value` 返回 owned `RuntimeValue` 导致的预检复制。编码仍保留精确长度、上限、CRC 和 canonical 验证。

验收：大 string/bytes/list/map/element batch 的编码过程不因预检产生与 payload 成比例的第二份值拷贝；round-trip、canonical rejection 和 resource-limit 测试保持通过。

### 4.2 Credit 驱动的 morsel exchange

把 `FragmentWorker` 的批量返回协议演进为有界的异步 morsel source。worker 仅在下游 credit 可用时执行、编码和发送下一批；coordinator 在接收 frame 后预留 frame 与 decoded upper bound，并在本地消费后释放 credit。empty stream 必须有显式 completion，不得使用伪空结果批。

每个在途 morsel 绑定 snapshot、fragment、schema、shard sequence 和 deadline；取消、capability drift、token mismatch 或 budget exceed 立即关闭 source 并释放 reservation。排序、聚合等阻塞算子仍使用自己的 memory/spill 边界，不能借 streaming 规避预算。

验收：首批输出不等待完整结果；在途内存上界由 credit 和 batch upper bound 推导；慢协调器不会迫使 worker 物化无限结果；取消后不再继续读取或发送。

### 4.3 Capability-aware access planning

优化器上下文携带 immutable capability snapshot 和 generation。物理 Scan、Expand、Change、PropertyGather access metadata 记录 primitive kind、planned guarantee 和 residual policy。`Unsupported` 选择现有 generic canonical path；`Candidate` 可使用 typed primitive 但强制 residual；第一次实现不删除 `Exact` residual。NodeScan 紧邻的安全属性比较可以作为有界候选提示下推，原始 Filter 始终保留；请求绑定 `valid_time`，后端不能证明无漏报时必须忽略提示。

计划 fingerprint 包含 capability generation。执行时 page guarantee 或 generation 与计划不匹配必须 fail closed 并重新计划，不能静默回退到语义未知的下推结果。

验收：计划测试能证明 Unsupported/Candidate/Exact 的路径选择、residual 保留和 generation drift 失效；Adapter 仍可使用任意内部布局。

### 4.4 页面合并、延迟物化与路由批量化

在 Adapter boundary 建立 request coalescer：同一 snapshot、primitive kind、shard、projection/fact demand 和预算域内的 point/property/adjacency 输入合并为一个 bounded request。按 shard 对 frontier 和 property keys 分桶；保留输入 ordinal 以恢复确定性结果。禁止跨 snapshot、security fingerprint、deadline 或 capability generation 合并。

PropertyGather 只接收 residual 后存活的键和 demanded property；完整实体投影才请求完整属性。对于小结果本地消费 canonical page，避免不必要的 row -> ColumnBatch -> frame 转换。

验收：相同查询结果下，RPC 数和读取字节不增加；批量 property/adjacency 访问保持顺序、分页、snapshot 和 budget 正确性。

### 4.5 Canonical fallback 批量化、并行 fanout 与编译复用

在尚未实现某个 typed primitive 的 Adapter 上，canonical fallback 仍必须避免 N+1 读取：把扫描得到的 identity、历史 projection、Expand 目标按 shard 和 page bounds 聚合为批量 `multi_get`，并在受限 in-flight 并发中执行。输出按原输入 ordinal 恢复确定性顺序，时态可见性、transaction overlay 和 residual 规则不变。

跨 shard 的独立请求必须并发启动，以 bounded fair fan-in 收集，不得在 gateway 或 coordinator 中按 shard 串行 `await`。同一 statement 的 read barrier、read view 和后续分页 primitive 必须绑定同一个经验证的 read session，避免对每页重复发起等价线性读；单次 `FencedScan` 仍不能冒充可分页 snapshot。

Gateway 到 engine 的 read path 必须只编译、绑定和优化一次。可缓存的 prepared physical template 使用规范化 query、参数类型、graph/schema/topology、security fingerprint 和 Adapter capability generation 作为键；SnapshotToken、deadline、实际参数值和 transaction overlay 只在执行时绑定，绝不跨 statement 复用。

验收：跨 shard point/property read 的 wall-clock 不再随 shard 数串行累加；generic Expand/scan 的后端读取数按 page 而不是按结果元素增长；一次执行不会重复编译/优化同一 query；任一缓存键或 read session 不匹配都 fail closed。

## 5. 指标与基准

建立可重复的性能矩阵，至少包括 memory、RocksDB/embedded、remote/sidecar 三种部署模式，以及：

- 单键点查、批量点查、1--3 hop 短路径；
- 单分片范围扫描和聚合；
- 多分片高基数扫描、聚合和 `CHANGES`；
- 小结果、大值结果、慢消费者和取消场景。

每个场景记录 p50/p95/p99、首批时间、吞吐、峰值 query/coordinator/worker memory、in-flight morsels、网络字节、encoded/decoded bytes、value-copy bytes、Adapter RPC 数、后端等待和 spill。直连后端测量仅作为基线；DTGProxy 的正确性和资源边界优先于为追平基线而破坏中间件职责。

每一个优化切片须先增加定性回归测试和可比较的微基准，再以基准结果决定是否扩大范围。没有基线、没有指标或使语义测试退化的“优化”不得合入。

## 6. 失败处理

优化只能减少工作，不能弱化失败边界。合并请求必须完整保留原请求的所有 budget、deadline、snapshot 和 security 约束；任何无法安全合并的输入独立执行。流式 source 的 frame/sequence 错误、超额页、token mismatch、checksum 失败、capability drift 和 cancellation 都必须终止该 query，释放 credit 和查询私有资源。

## 7. 实施顺序

1. 增加 performance counters 与最小基准 harness，修复 exchange 借用访问的双重复制。
2. 完成 credit-driven morsel exchange、并发 shard fanout 和背压/cancellation 测试。
3. 完成 canonical fallback 的 shard-batched materialization 与 read-session 复用。
4. 完成 capability-aware physical access metadata 与 optimizer 使用路径。
5. 实现 request coalescer、delayed PropertyGather 与 prepared physical template 缓存。
6. 用完整矩阵比较各阶段，只有达到资源边界和正确性要求时才继续扩展到更多 Adapter primitive。
