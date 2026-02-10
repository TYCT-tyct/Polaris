# Polaris Module1 操作指南（DataHarvester）

## 1. 这套服务在做什么
- Module1 是数据底座，负责持续采集：
  - XTracker 用户信息、追踪窗口、推文正文与计数
  - Gamma 市场与 token
  - CLOB top/depth/L2 行情
  - 分钟聚合与健康统计
- 目标是为后续 Module2~4 提供稳定、可回放、可审计的数据。

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
