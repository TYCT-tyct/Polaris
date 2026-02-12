# Polaris Module2 运维指南

## 1. 准备
1. 先运行迁移：
   `python -m polaris.cli migrate`
2. 检查数据源：
   `python -m polaris.cli doctor --handle elonmusk`
3. 确认 Module1 正在采集市场与盘口。

## 2. 启动方式
- 影子模式：
  `python -m polaris.cli arb-run --mode shadow --source polymarket_shadow --run-tag auto`
- 实时 paper：
  `python -m polaris.cli arb-run --mode paper_live --source polymarket_shared10 --paper-capital-scope shared --run-tag auto`
- 实盘：
  `python -m polaris.cli arb-run --mode live --source polymarket_live --run-tag auto`

独立资金池（每个策略独立 10 美元）：
`python -m polaris.cli arb-run --mode paper_live --source polymarket_isolated10 --paper-capital-scope strategy`

共享资金池（所有策略共享 10 美元）：
`python -m polaris.cli arb-run --mode paper_live --source polymarket_shared10 --paper-capital-scope shared`

一键同时启动“独立+共享”两套后台 paper：
`python -m polaris.cli arb-paper-matrix-start --duration-hours 8 --bankroll-usd 10 --source-prefix polymarket`

停止一键启动的两套后台 paper：
`python -m polaris.cli arb-paper-matrix-stop`

## 2.1 Rust 低抖动桥接（可选）
默认关闭。启用后，paper/replay 的订单簿撮合计算走 Rust 二进制，降低 Python GC 抖动对延迟尾部的影响。

1. 编译 Rust 二进制（服务器执行）：
   `cd rust/polaris_book_sim && cargo build --release`
2. 配置 `.env`：
   `POLARIS_ARB_RUST_BRIDGE_ENABLED=true`
   `POLARIS_ARB_RUST_BRIDGE_MODE=daemon`
   `POLARIS_ARB_RUST_BRIDGE_BIN=/home/ubuntu/polaris/rust/polaris_book_sim/target/release/polaris-book-sim`
   `POLARIS_ARB_RUST_BRIDGE_TIMEOUT_SEC=5`
3. 重启 `arb-run` 进程并观察日志是否出现 rust bridge 启动记录。

### Python / Rust 切换
- 纯 Python 路径（默认，最稳）：
  `POLARIS_ARB_RUST_BRIDGE_ENABLED=false`
- Rust 常驻服务路径（推荐）：
  `POLARIS_ARB_RUST_BRIDGE_ENABLED=true`
  `POLARIS_ARB_RUST_BRIDGE_MODE=daemon`
- Rust 单次子进程路径（仅兼容调试，不推荐）：
  `POLARIS_ARB_RUST_BRIDGE_ENABLED=true`
  `POLARIS_ARB_RUST_BRIDGE_MODE=subprocess`
- Rust PyO3 同进程路径（推荐做低抖动基准对比）：
  1. 安装构建工具：`pip install maturin`
  2. 编译并安装扩展模块：`cd rust/polaris_pyo3 && maturin develop --release`
  3. 启用模式：
     `POLARIS_ARB_RUST_BRIDGE_ENABLED=true`
     `POLARIS_ARB_RUST_BRIDGE_MODE=pyo3`

说明：`pyo3` 避免了 subprocess 的 IPC 序列化开销，适合高频更新场景；若 `polaris_rs` 未安装，系统会自动回退到 Python 模拟路径，不会中断。

## 3. 回放验证
默认高速回放（推荐）：
`python -m polaris.cli arb-replay --start 2026-02-10T00:00:00+00:00 --end 2026-02-10T06:00:00+00:00 --fast`

完整审计回放（会写入明细信号/订单/成交，速度更慢）：
`python -m polaris.cli arb-replay --start 2026-02-10T00:00:00+00:00 --end 2026-02-10T06:00:00+00:00 --full`

手动触发参数进化：
`python -m polaris.cli arb-optimize`

## 4. 报表与导出
- 聚合报表：
  `python -m polaris.cli arb-report --group-by strategy,mode,source --run-tag current`
- 隔夜策略总结（推荐早晨查看）：
  `python -m polaris.cli arb-summary --since-hours 12 --mode paper_live --source polymarket_shared10 --run-tag current`
- 查看历史全部版本（用于横向对比）：
  `python -m polaris.cli arb-summary --since-hours 24 --mode paper_live --source all --run-tag all`
- 清理旧版本污染数据（先预览）：
  `python -m polaris.cli arb-clean --mode paper_live --source all --run-tag all --since-hours 0 --dry-run`
- 清理旧版本污染数据（执行删除）：
  `python -m polaris.cli arb-clean --mode paper_live --source all --run-tag all --since-hours 0 --apply`
- 导出隔夜总结到文件：
  `python -m polaris.cli arb-summary --since-hours 12 --mode paper_live --source polymarket_shared10 --output exports/overnight_summary_shared.json`
  `python -m polaris.cli arb-summary --since-hours 12 --mode paper_live --source polymarket_isolated10 --output exports/overnight_summary_isolated.json`
- 导出：
  `python -m polaris.cli arb-export --table arb_trade_result --format csv --since-hours 24`
- 基准压测（延迟 p50/p95）：
  `python -m polaris.cli arb-benchmark --mode paper_live --rounds 30 --warmup 3`
- T2T/Jitter 基准（真实订单簿更新口径）：
  `python -m polaris.cli arb-benchmark-t2t --backend both --iterations 120 --updates 1000 --levels-per-side 250 --output benchmarks/t2t_compare.json`
  `python -m polaris.cli arb-benchmark-t2t --backend all --iterations 120 --updates 1000 --levels-per-side 250 --output benchmarks/t2t_all_compare.json`

`arb-summary` 输出重点：
- `totals.signals_found`：发现机会总数。
- `totals.signals_executed`：实际执行信号总数。
- `totals.trades`：完成交易总数。
- `totals.net_pnl_usd`：净盈亏。
- `totals.turnover_usd`：资金周转量。
- `totals.execution_rate`：执行率（执行信号/发现信号）。
- `totals.trade_conversion_rate`：交易转化率（交易数/执行信号）。
- `by_strategy[*]`：按策略拆分的同维度统计，含 `win_rate`、`avg_detection_to_event_ms`、`p95_detection_to_event_ms`。

## 5. 常见排查
- 无信号：先检查 `dim_market` 和 `dim_token` 是否有 active 市场。
- A/C 无信号：确认 `dim_market.neg_risk=true` 的市场是否存在。A/C 仅对 NegRisk 组扫描，不再对普通二元市场误扫。
- 有信号不执行：检查 `arb_risk_event` 的拒单原因。
- paper 有成交但 live 无成交：检查 `POLARIS_ARB_LIVE_PRIVATE_KEY` 和链路权限。

## 6. 热更新
- 修改 `.env` 后，重启进程或发送 `SIGHUP`。
- 关键参数变更会体现在 `arb_param_snapshot`。

## 7. 低延迟参数建议（小资金）
- `POLARIS_ARB_MAX_SIGNALS_PER_CYCLE=48`：限制每轮处理信号上限，避免队列过长。
- `POLARIS_ARB_UNIVERSE_MAX_HOURS=72`：只扫描 72 小时内到期市场，提高周转效率。
- `POLARIS_ARB_UNIVERSE_TOKEN_LIMIT=2000`：限制每轮 token 上限，控制请求体积。
- `POLARIS_ARB_UNIVERSE_REFRESH_SEC=180`：Universe 元数据缓存秒数，降低每轮数据库压力。
- `POLARIS_ARB_UNIVERSE_MIN_LIQUIDITY=0`：按流动性过滤低质量市场，建议实盘设为 `50~200`。
- `POLARIS_ARB_C_MAX_CANDIDATES_PER_EVENT=1`：策略 C 每个事件只执行最优候选，减少重复腿。
- `POLARIS_ARB_CLOB_BOOKS_BATCH_SIZE=500`：`/books` 批量大小，过大可能触发 400。
- `POLARIS_ARB_CLOB_BOOKS_MAX_CONCURRENCY=4`：并发批次上限，过高会放大网络抖动。
- `POLARIS_CLOB_HTTP2_ENABLED=true`：启用 HTTP/2 复用，降低连接抖动和握手开销。
- `POLARIS_CLOB_MAX_CONNECTIONS=80`：CLOB 客户端连接池上限。
- `POLARIS_CLOB_MAX_KEEPALIVE_CONNECTIONS=40`：保活连接上限，减少反复建连。
- `POLARIS_CLOB_WS_ENABLED=true`：启用 CLOB WebSocket 市场流。
- `POLARIS_CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market`：官方 market 订阅端点。
- `POLARIS_CLOB_WS_BOOK_MAX_AGE_SEC=2.5`：WS 缓存最大年龄，超时自动回退 REST `/books`。
- `POLARIS_CLOB_WS_MAX_SUBSCRIBE_TOKENS=3500`：单进程订阅上限，超过上限走 REST。
- `POLARIS_CLOB_WS_RECONNECT_MIN_SEC=0.5` / `POLARIS_CLOB_WS_RECONNECT_MAX_SEC=8`：断线重连退避区间。
- `POLARIS_ARB_EXECUTION_CONCURRENCY=3`：单轮并发执行信号数，提升多机会同时捕获能力。
- `POLARIS_ARB_LIVE_PREFLIGHT_MAX_AGE_MS=2000`：Live 预检快照最大复用时长，过期才二次拉盘口。
- `POLARIS_ARB_LIVE_PREFLIGHT_FORCE_REFRESH=false`：是否强制每单二次拉盘口，默认关闭以减少延迟。
- `POLARIS_ARB_RUST_BRIDGE_ENABLED=false`：默认关闭，稳定后再切到 `true`。
- 说明：系统已内置 WS->REST 双通道。WS 命中时跳过 REST；WS 缺失或过期会自动回退 `/books`，并保留 400/413 自动拆分回退。

## 8. T2T 基准解读
- `avg_update_us`：每个 tick 的平均处理耗时（越低越好）。
- `p95_update_us/p99_update_us`：延迟尾部与抖动（实盘更关注）。
- `decode_ms`：JSON 解包成本。
- `process_ms`：订单簿更新+spread/depth 计算成本。
- `rust_vs_python_speedup`：`>1` 表示 Rust 更快，`<1` 表示 Python 更快。
