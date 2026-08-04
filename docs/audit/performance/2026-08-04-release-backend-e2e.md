# 2026-08-04 release 后端 E2E 审计

每个 provider 独立运行真实 Bolt → Gateway → Data → Raft → provider 路径：5 个 T-Cypher
工作负载、并发 1/8/64、每 cell 预热 1 秒、计量 5 秒、重复 3 次，共 45 cell。所有 artifact
均为零错误；单条 CREATE 另以 `COUNT(*) == warmup + measured` 审计落盘。

原始样本不提交（`target/` 被忽略），但可用以下 SHA-256 复核本次本机生成的 artifact：

| 后端 | artifact | revision | SHA-256 |
| --- | --- | --- | --- |
| Fjall | `fjall-r3.json` | `a73f1ec` | `7ddc49b60e5593314a559316cefb592734ed9e83d5a3d2fc707e542d876e345c` |
| Kuzu | `kuzu-r1.json` | `a73f1ec` | `b41dc3f57d5bab742bc2b3eed5c81fcbd312840a37ec0912ba53b02d2988a889` |
| PostgreSQL | `postgresql-r2.json` | `fd03bf2` | `646413d36cd171ec76d0aa4e0d1dd34e7eeae8ee93997e0820f4f5fde0a1f8f0` |

`fd03bf2` 仅在预热前固定 Data/Gateway 指标基线，未修改执行或计时路径；Fjall/Kuzu 的吞吐与
延迟仍可复现，但其阶段均值不与 PostgreSQL 混合比较。PostgreSQL 使用临时随机凭据、随机端口、
仅 `127.0.0.1` 监听的实例，进程退出后已停止并清理。

复现命令：

```bash
cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
DTG_BACKEND_E2E_SELECTED_BACKEND=fjall \
DTG_BACKEND_E2E_QUICK_REPETITIONS=3 \
DTG_BACKEND_E2E_QUICK_OUTPUT="$PWD/target/backend-e2e-release-20260804/fjall.json" \
cargo test --locked --release -p dtg-gateway --test backend_e2e_diagnostic \
  quick_selected_backend_e2e_comparison -- --ignored --exact --nocapture
```

将 `fjall` 替换为 `kuzu` 或 `postgresql`；后者必须提供一个临时 loopback endpoint 与凭据。
