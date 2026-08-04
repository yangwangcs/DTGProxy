# 2026-08-04 release 后端 E2E 审计

每个 provider 独立运行真实 Bolt → Gateway → Data → Raft → provider 路径：5 个 T-Cypher
工作负载、并发 1/8/64、每 cell 预热 1 秒、计量 5 秒、重复 3 次，共 45 cell。所有 artifact
均为零错误；单条 CREATE 另以 `COUNT(*) == warmup + measured` 审计落盘。

原始样本不提交（`target/` 被忽略），但可用以下 SHA-256 复核本次本机生成的 artifact：

| 后端 | artifact | revision | SHA-256 |
| --- | --- | --- | --- |
| Fjall | `fjall-final.json` | `0c6467b` | `5902e08e8e9aec307e7575b38d97e26a5b5cd3a6d3aa70ac7d7b0bad3049fb55` |
| Kuzu | `kuzu-final.json` | `0c6467b` | `4f535a1ed467cc499145adf5e0306e56f7d7c77d286b0260993ce8e5ff4df53c` |
| PostgreSQL | `postgresql-final.json` | `0c6467b` | `20b8e8fa87e0364476c97e3d2dc172a38beeff323ea7489851a55145e4f80097` |

三份 artifact 均在包含显式预热前 Data/Gateway 指标基线和有界 worker drain 的当前提交上生成。
PostgreSQL 使用临时随机凭据、随机端口、仅 `127.0.0.1` 监听的实例，进程退出后已停止并清理。

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
