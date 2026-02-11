# Polaris 架构镜像

## 目录结构
```text
.
├─ .env.example
├─ .gitignore
├─ pyproject.toml
├─ Makefile
├─ CLAUDE.md
├─ docs/
│  ├─ module1_dataharvester_design.md
│  ├─ module1_operations_guide.md
│  ├─ module2_implementation_plan.md
│  ├─ module2_operations_guide.md
│  ├─ data_dictionary.md
│  └─ aws_ubuntu_runbook.md
├─ polaris/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ logging.py
│  ├─ cli.py
│  ├─ db/
│  │  ├─ __init__.py
│  │  ├─ pool.py
│  │  └─ migrations/
│  │     ├─ 0001_init.sql
│  │     ├─ 0002_partitions_indexes.sql
│  │     ├─ 0003_module2_core.sql
│  │     ├─ 0004_module2_views.sql
│  │     └─ 0005_module2_run_tag_indexes.sql
│  ├─ infra/
│  │  ├─ __init__.py
│  │  ├─ rate_limiter.py
│  │  ├─ retry.py
│  │  └─ scheduler.py
│  ├─ sources/
│  │  ├─ __init__.py
│  │  ├─ models.py
│  │  ├─ xtracker_client.py
│  │  ├─ gamma_client.py
│  │  └─ clob_client.py
│  ├─ harvest/
│  │  ├─ __init__.py
│  │  ├─ discovery.py
│  │  ├─ collector_tweets.py
│  │  ├─ collector_markets.py
│  │  ├─ collector_quotes.py
│  │  ├─ mapper_market_tracking.py
│  │  └─ runner.py
│  ├─ arb/
│  │  ├─ __init__.py
│  │  ├─ contracts.py
│  │  ├─ config.py
│  │  ├─ orchestrator.py
│  │  ├─ cli.py
│  │  ├─ reporting.py
│  │  ├─ bench/
│  │  │  ├─ __init__.py
│  │  │  └─ t2t.py
│  │  ├─ strategies/
│  │  │  ├─ __init__.py
│  │  │  ├─ common.py
│  │  │  ├─ strategy_a.py
│  │  │  ├─ strategy_b.py
│  │  │  ├─ strategy_c.py
│  │  │  ├─ strategy_f.py
│  │  │  └─ strategy_g.py
│  │  ├─ execution/
│  │  │  ├─ __init__.py
│  │  │  ├─ fill_simulator.py
│  │  │  ├─ sizer.py
│  │  │  ├─ risk_gate.py
│  │  │  ├─ order_router.py
│  │  │  └─ rust_bridge.py
│  │  └─ ai/
│  │     ├─ __init__.py
│  │     ├─ gate.py
│  │     ├─ provider_google.py
│  │     ├─ provider_anthropic.py
│  │     ├─ provider_openai.py
│  │     ├─ provider_minimax.py
│  │     └─ provider_zhipu.py
│  └─ ops/
│     ├─ __init__.py
│     ├─ backup.py
│     ├─ exporter.py
│     ├─ health.py
│     └─ backfill.py
├─ rust/
│  ├─ polaris_book_sim/
│  │  ├─ Cargo.toml
│  │  └─ src/
│  │     └─ main.rs
│  └─ polaris_pyo3/
│     ├─ Cargo.toml
│     └─ src/
│        └─ lib.rs
└─ tests/
   ├─ conftest.py
   ├─ test_discovery.py
   ├─ test_gamma_parsing.py
   ├─ test_post_incremental.py
   ├─ test_rate_limiter.py
   ├─ test_retry.py
   ├─ test_mapping_score.py
   ├─ test_exporter.py
   ├─ test_backup.py
   ├─ test_upsert_idempotency.py
   ├─ test_scheduler.py
   ├─ test_live_sources.py
   ├─ test_live_harvest_once.py
   ├─ test_arb_live_scan.py
   ├─ test_arb_replay_real.py
   ├─ test_arb_reporting.py
   ├─ test_order_router_preflight.py
   └─ test_t2t_benchmark.py
```

## 模块职责
- `polaris/config.py`：集中管理 Module1 + Module2 的运行参数与开关。
- `polaris/harvest/*`：Module1 采集层，负责事实数据落库。
- `polaris/arb/*`：Module2 套利引擎，统一编排扫描、风控、执行、回放、参数进化。
- `polaris/arb/strategies/*`：策略检测器（A/B/C/F/G）。
- `polaris/arb/execution/*`：执行路由、成交模拟、仓位与风险控制。
- `polaris/arb/execution/rust_bridge.py`：可选 Rust 二进制桥接，承载低抖动 paper/replay 撮合热路径。
- `polaris/arb/bench/t2t.py`：T2T/Jitter 基准，覆盖 JSON 解包、订单簿更新与深度计算。
- `polaris/arb/ai/*`：G 策略可选 AI 复核层（google/claude/gpt/minimax/zhipu）。
- `polaris/ops/exporter.py`：统一导出 Module1/2 关键表与视图。
- `rust/polaris_book_sim`：Rust 订单簿撮合模拟器，供 Module2 可选调用。
- `rust/polaris_pyo3`：PyO3 同进程扩展模块，减少 IPC 开销并降低 T2T 抖动。

## 依赖边界
- 采集链路：`cli -> harvest.runner -> collectors -> sources/db`。
- 套利链路：`cli -> arb.orchestrator -> strategies -> risk_gate -> order_router -> db`。
- `sources` 仅做外部接口封装，不承载策略决策。
- `arb` 与 `harvest` 通过数据库事实表耦合，不直接跨模块调用业务逻辑。

## 关键设计决策
- 市场发现从硬编码 Elon 迁移为 `scope=all|elon_tweet`，默认全市场。
- Module2 全链路统一记录：信号、订单、成交、风险、PnL、参数版本、回放指标。
- C 策略完整实现但默认禁止实盘，符合小资金风险边界。
- G 策略支持规则锁定 + 可选 AI 复核，AI 不可用时自动退化为纯规则。
- 参数进化基于“历史回放 + 最近 24h paper”双评分，所有版本可追溯。
- A/C 策略扫描范围收敛为 NegRisk 市场组，避免在普通二元市场产生伪机会。
- 持有型策略在开仓时使用 `entry_only` 收益口径，避免把未实现浮盈亏误计为已实现亏损。
- 引入 Rust 桥接可选路径，支持 `daemon/subprocess/pyo3` 三种模式，默认关闭，稳定后按环境开关灰度开启。
- Module2 全链路引入 `run_tag` 隔离（signal/trade/risk/cash/replay），报表默认看 `current`，避免旧版本数据污染当前收益判断。

## 变更日志
- 2026-02-10：新增 Module2 迁移 `0003/0004` 与 `arb_*` 数据模型。
- 2026-02-10：新增 Module2 核心包 `polaris/arb`（ABFG 实盘路径，C 默认禁实盘）。
- 2026-02-10：新增回放引擎、报表导出、参数进化与 AI 复核接口。
- 2026-02-10：市场发现支持全市场范围并保持 `elon_tweet` 兼容模式。
- 2026-02-11：新增 `arb-summary` 隔夜总结命令与 `ArbReporter.summary` 统计指标增强。
- 2026-02-11：新增 `tests/test_arb_reporting.py`，验证策略级发现/执行/盈亏/延迟口径。
- 2026-02-11：优化 Module2 执行链路并发与 Live 预检复用，新增 `tests/test_order_router_preflight.py`。
- 2026-02-12：修复 `.env` 热更新覆盖语义、A/C NegRisk 分组过滤、持有型策略收益口径统一。
- 2026-02-12：新增 `rust/polaris_book_sim` 与 `rust_bridge` 可选加速路径。
- 2026-02-12：新增 `arb-benchmark-t2t` 与 Rust `bench-orderbook` 子命令，用于真实 tick 更新口径对比。
- 2026-02-12：新增 `rust/polaris_pyo3` 与 `pyo3` 执行模式，支持同进程 Rust 基准与 paper 模拟。
- 2026-02-12：新增 `0005_module2_run_tag_indexes.sql`、`arb-clean` 命令和 `run_tag` 报表过滤，修复旧配置混入历史报表问题。
