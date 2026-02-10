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
│  │     └─ 0004_module2_views.sql
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
│  │  │  └─ order_router.py
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
   └─ test_arb_replay_real.py
```

## 模块职责
- `polaris/config.py`：集中管理 Module1 + Module2 的运行参数与开关。
- `polaris/harvest/*`：Module1 采集层，负责事实数据落库。
- `polaris/arb/*`：Module2 套利引擎，统一编排扫描、风控、执行、回放、参数进化。
- `polaris/arb/strategies/*`：策略检测器（A/B/C/F/G）。
- `polaris/arb/execution/*`：执行路由、成交模拟、仓位与风险控制。
- `polaris/arb/ai/*`：G 策略可选 AI 复核层（google/claude/gpt/minimax/zhipu）。
- `polaris/ops/exporter.py`：统一导出 Module1/2 关键表与视图。

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

## 变更日志
- 2026-02-10：新增 Module2 迁移 `0003/0004` 与 `arb_*` 数据模型。
- 2026-02-10：新增 Module2 核心包 `polaris/arb`（ABFG 实盘路径，C 默认禁实盘）。
- 2026-02-10：新增回放引擎、报表导出、参数进化与 AI 复核接口。
- 2026-02-10：市场发现支持全市场范围并保持 `elon_tweet` 兼容模式。
