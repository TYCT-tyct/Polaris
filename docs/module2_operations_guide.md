# Polaris Module2 运维指南

## 1. 准备
1. 先运行迁移：
   `python -m polaris.cli migrate`
2. 检查数据源：
   `python -m polaris.cli doctor --handle elonmusk`
3. 确认 Module1 正在采集市场与盘口。

## 2. 启动方式
- 影子模式：
  `python -m polaris.cli arb-run --mode shadow`
- 实时 paper：
  `python -m polaris.cli arb-run --mode paper_live`
- 实盘：
  `python -m polaris.cli arb-run --mode live`

## 3. 回放验证
默认高速回放（推荐）：
`python -m polaris.cli arb-replay --start 2026-02-10T00:00:00+00:00 --end 2026-02-10T06:00:00+00:00 --fast`

完整审计回放（会写入明细信号/订单/成交，速度更慢）：
`python -m polaris.cli arb-replay --start 2026-02-10T00:00:00+00:00 --end 2026-02-10T06:00:00+00:00 --full`

手动触发参数进化：
`python -m polaris.cli arb-optimize`

## 4. 报表与导出
- 聚合报表：
  `python -m polaris.cli arb-report --group-by strategy,mode,source`
- 导出：
  `python -m polaris.cli arb-export --table arb_trade_result --format csv --since-hours 24`
- 基准压测（延迟 p50/p95）：
  `python -m polaris.cli arb-benchmark --mode paper_live --rounds 30 --warmup 3`

## 5. 常见排查
- 无信号：先检查 `dim_market` 和 `dim_token` 是否有 active 市场。
- 有信号不执行：检查 `arb_risk_event` 的拒单原因。
- paper 有成交但 live 无成交：检查 `POLARIS_ARB_LIVE_PRIVATE_KEY` 和链路权限。

## 6. 热更新
- 修改 `.env` 后，重启进程或发送 `SIGHUP`。
- 关键参数变更会体现在 `arb_param_snapshot`。

## 7. 低延迟参数建议（小资金）
- `POLARIS_ARB_MAX_SIGNALS_PER_CYCLE=48`：限制每轮处理信号上限，避免队列过长。
- `POLARIS_ARB_UNIVERSE_MAX_HOURS=72`：只扫描 72 小时内到期市场，提高周转效率。
- `POLARIS_ARB_UNIVERSE_TOKEN_LIMIT=2000`：限制每轮 token 上限，控制请求体积。
- `POLARIS_ARB_C_MAX_CANDIDATES_PER_EVENT=1`：策略 C 每个事件只执行最优候选，减少重复腿。
