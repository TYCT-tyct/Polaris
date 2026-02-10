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
│  │     └─ 0002_partitions_indexes.sql
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
│  └─ ops/
│     ├─ __init__.py
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
   ├─ test_upsert_idempotency.py
   ├─ test_live_sources.py
   └─ test_live_harvest_once.py
```

## 模块职责
- `polaris/config.py`：所有运行参数的唯一入口，避免分散常量。
- `polaris/cli.py`：运维入口，负责组装运行时依赖并触发命令。
- `polaris/db/pool.py`：数据库连接池、迁移执行、通用读写。
- `polaris/infra/*`：限流、重试、调度三件基础设施。
- `polaris/sources/*`：对外部 API 的稳定封装，隐藏上游形态波动。
- `polaris/harvest/*`：Module 1 采集逻辑与编排，专注数据而非策略。
- `polaris/ops/*`：健康聚合与补采流程。
- `docs/*`：设计、字典、部署手册。

## 依赖边界
- 依赖单向流：`cli -> runner -> collectors -> clients/db`。
- `sources` 只做数据获取与解析，不做业务决策。
- `harvest` 只做规则与入库，不承担执行策略。
- `ops` 只做可观测与运维，不修改核心采集逻辑。

## 关键设计决策
- 官方付费 X API 被明确排除，正文默认来自 XTracker `posts` 接口。
- 高频事实表分区 + BRIN 时间索引，保证采集与查询同时可扩展。
- 原始高频 14 天保留，分钟聚合长期保留，成本与回放能力平衡。
- 所有写库路径都使用幂等 upsert，重复采集不产生脏数据。
- 每个采集任务都落 `ops_collector_run`，故障可追溯。

## 变更日志
- 2026-02-10：从零重建 `Polaris` 基础设施与 Module 1。
- 2026-02-10：启用正文增量采集（hash + last_post_id 双门控）。
- 2026-02-10：补齐在线来源测试与端到端 `harvest-once` 测试。

