# Polaris Module1 操作指南（DataHarvester）

## 1. 这套服务在做什么
- Module1 是数据底座，负责持续采集：
  - XTracker 用户信息、追踪窗口、推文正文与计数
  - Gamma 市场与 token
  - CLOB top/depth/L2 行情
  - 分钟聚合与健康统计
- 目标是为后续 Module2~4 提供稳定、可回放、可审计的数据。

## 1.1 采集数据总表
| 表/视图 | 类型 | 核心字段 | 来源 | 频率/触发 | 主要用途 |
|---|---|---|---|---|---|
| `dim_account` | 维表 | `account_id`, `handle`, `post_count`, `last_sync` | XTracker users | `tracking_sync` / `post_sync` | 账号主数据 |
| `dim_tracking_window` | 维表 | `tracking_id`, `account_id`, `start_date`, `end_date` | XTracker trackings | `tracking_sync` | 市场窗口对齐 |
| `fact_tweet_metric_daily` | 事实表 | `metric_date`, `tweet_count`, `cumulative_count` | XTracker metrics | `metric_sync` | 日级计数曲线 |
| `fact_tweet_post` | 事实表 | `platform_post_id`, `content`, `posted_at`, `is_reply` | XTracker posts | `post_sync` | 正文与语义特征 |
| `dim_market` | 维表 | `market_id`, `condition_id`, `question`, `active` | Gamma markets | `markets_discovery` | 市场元数据 |
| `dim_token` | 维表 | `token_id`, `market_id`, `outcome_label` | Gamma outcomes | `markets_discovery` | token 映射 |
| `bridge_market_tracking` | 关系表 | `market_id`, `tracking_id`, `match_score` | 内部映射 | `mapping_sync` | 市场-窗口关联 |
| `fact_market_state_snapshot` | 事实表 | `active`, `closed`, `spread`, `liquidity` | Gamma markets | `markets_discovery` | 市场状态时序 |
| `fact_quote_top_raw` | 高频事实 | `best_bid`, `best_ask`, `mid`, `spread` | CLOB book | `quote_top_sync` | 顶层盘口序列 |
| `fact_quote_depth_raw` | 高频事实 | `bid_depth_*`, `ask_depth_*`, `imbalance` | CLOB book | `quote_depth_sync` | 深度与失衡 |
| `fact_orderbook_l2_raw` | 高频事实 | `side`, `price`, `size`, `level_index` | CLOB book | `orderbook_l2_sync` | L2 回放 |
| `fact_quote_1m` | 聚合事实 | `open/high/low/close`, `sample_count` | 原始行情聚合 | `agg_1m` | 分钟级策略输入 |
| `fact_settlement` | 事实表 | `winning_outcome`, `winning_token_id`, `settled_at` | 结算补录 | 结算后写入 | 训练与复盘 |
| `ops_collector_run` | 运维事实 | `job_name`, `status`, `latency_ms` | 内部 | 每次任务执行 | 运行审计 |
| `ops_api_health_minute` | 运维聚合 | `runs`, `error_runs`, `p95_latency_ms` | `ops_collector_run` 聚合 | `health_agg` | 稳定性监控 |
| `ops_cursor` | 游标 | `cursor_key`, `cursor_value`, `updated_at` | 内部 | 增量写入时 | 去重与断点续采 |
| `view_quote_latest` | 视图 | 每 token 最新 bid/ask/mid | `fact_quote_top_raw` | 实时查询 | 低延迟读取 |

## 2. `systemd` 的作用
- 让服务以守护进程方式长期运行（无需保持终端会话）。
- 开机自动启动（`enabled`）。
- 进程异常退出自动重启（`Restart=always`）。
- 统一日志入口（`journalctl -u polaris-harvest`）。

结论：只要机器在线、`polaris-harvest` 处于 `active`，就是 7x24 持续采集。

## 3. 当前服务器部署路径（你的机器）
- 目录：`/home/ubuntu/polaris`
- 服务：`polaris-harvest.service`
- 虚拟环境：`/home/ubuntu/polaris/.venv`
- 配置文件：`/home/ubuntu/polaris/.env`

## 4. 日常运维命令
```bash
# 连接服务器
ssh -i "C:\Users\Shini\Documents\terauss.pem" ubuntu@108.130.51.215

# 查看服务状态
sudo systemctl status polaris-harvest

# 是否开机自启
sudo systemctl is-enabled polaris-harvest

# 是否正在运行
sudo systemctl is-active polaris-harvest

# 实时日志
sudo journalctl -u polaris-harvest -f

# 重启服务（改完配置后使用）
sudo systemctl restart polaris-harvest

# 热更新（尽量不断服务，触发进程内重载）
sudo systemctl reload polaris-harvest
```

## 5. 手工执行 CLI（一次性任务）
注意：手工跑命令前必须导入 `.env`，否则会使用默认数据库地址。

```bash
cd /home/ubuntu/polaris
source .venv/bin/activate
set -a
source .env
set +a

python -m polaris.cli migrate
python -m polaris.cli doctor --handle elonmusk
python -m polaris.cli harvest-once --handle elonmusk
```

## 6. 判断“是否在持续采集”
进入数据库后看最近数据是否增长：

```bash
cd /home/ubuntu/polaris
set -a; source .env; set +a

python - <<'PY'
import os
import psycopg

dsn = os.environ["POLARIS_DATABASE_URL"]
qs = {
    "ops_runs_5m": "select count(*) from ops_collector_run where started_at >= now() - make_interval(mins => 5)",
    "quote_top_5m": "select count(*) from fact_quote_top_raw where captured_at >= now() - make_interval(mins => 5)",
    "l2_5m": "select count(*) from fact_orderbook_l2_raw where captured_at >= now() - make_interval(mins => 5)",
    "post_total": "select count(*) from fact_tweet_post",
}
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        for k, q in qs.items():
            cur.execute(q)
            print(f"{k}={cur.fetchone()[0]}")
PY
```

如果 `ops_runs_5m`、`quote_top_5m`、`l2_5m` 持续 > 0，说明采集在工作。

## 7. 修改采集频率
编辑 `.env` 对应参数，然后重启服务。

常用参数：
- `POLARIS_QUOTE_TOP_SYNC_INTERVAL`（默认 10 秒）
- `POLARIS_QUOTE_DEPTH_SYNC_INTERVAL`（默认 30 秒）
- `POLARIS_ORDERBOOK_L2_SYNC_INTERVAL`（默认 60 秒）
- `POLARIS_POST_SYNC_INTERVAL`（默认 120 秒）
- `POLARIS_METRIC_SYNC_INTERVAL`（默认 120 秒）

```bash
cd /home/ubuntu/polaris
vim .env
sudo systemctl restart polaris-harvest
```

## 8. 升级代码流程（拉新版本）
```bash
cd /home/ubuntu/polaris
git pull --ff-only
source .venv/bin/activate
pip install -e .
set -a; source .env; set +a
python -m polaris.cli migrate
sudo systemctl restart polaris-harvest
```

## 8.1 热更新配置（不改代码）
适用：你只改 `.env`（采样频率、限流、是否启用 L2 等）。

```bash
cd /home/ubuntu/polaris
vim .env
sudo systemctl reload polaris-harvest
```

说明：
- 当前 `run` 默认开启 `--hot-reload`。
- 触发 `reload` 后，进程会平滑重建运行时并加载新配置。
- 若遇到异常，退回 `sudo systemctl restart polaris-harvest`。

## 8.2 导出数据（下载查看）
### 查看支持导出的表
```bash
cd /home/ubuntu/polaris
source .venv/bin/activate
set -a; source .env; set +a
python -m polaris.cli export-tables
```

### 导出为 CSV
```bash
python -m polaris.cli export \
  --table fact_quote_1m \
  --format csv \
  --since-hours 24 \
  --limit 50000 \
  --output exports/fact_quote_1m_24h.csv
```

### 导出为 JSON
```bash
python -m polaris.cli export \
  --table fact_tweet_post \
  --format json \
  --since-hours 48 \
  --limit 10000 \
  --output exports/fact_tweet_post_48h.json
```

### 从服务器下载到本地
```bash
scp -i "C:\Users\Shini\Documents\terauss.pem" \
  ubuntu@108.130.51.215:/home/ubuntu/polaris/exports/fact_quote_1m_24h.csv \
  .
```

## 9. 常见故障与处理
### 9.1 服务是 `failed`
```bash
sudo systemctl status polaris-harvest
sudo journalctl -u polaris-harvest -n 200 --no-pager
```

### 9.2 数据库连接失败（密码/DSN）
- 检查 `.env` 的 `POLARIS_DATABASE_URL`。
- 用 `psql` 或 `python + psycopg` 先验证可连。
- 修复后 `sudo systemctl restart polaris-harvest`。

### 9.3 手工命令连到默认端口 `55432`
- 原因是没 `source .env`。
- 按第 5 节先导入环境再执行。

## 10. 安全建议
- `.env` 含数据库密码，不要提交到 Git。
- 建议定期轮换数据库密码：
  - `ALTER ROLE polaris WITH PASSWORD '...';`
  - 同步更新 `/home/ubuntu/polaris/.env`
  - 重启服务。
