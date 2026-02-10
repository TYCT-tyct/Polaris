# Polaris Module2 实施计划

## 目标
- 全市场套利引擎，支持 A/B/F/G 实盘。
- C 策略完整实现，默认禁用实盘。
- 统一支持 shadow、paper_live、paper_replay、live。
- 交易全链路可追溯，可导出，可复盘。

## 架构
`Universe -> Scanner -> RiskGate -> OrderRouter -> Recorder -> Reporter`

## 策略
- A: NegRisk 多 outcome 加总套利。
- B: Binary YES+NO 套利。
- F: 高概率短期限资金停泊。
- G: 临近结算收敛交易（可选 AI 复核）。
- C: NO->YES 转换套利（默认 live=false）。

## 数据
- 新增 `arb_*` 系列表，覆盖信号、订单、成交、仓位、资金、回放、参数快照。
- 新增视图：`view_arb_pnl_daily`、`view_arb_by_strategy_mode_source`、`view_arb_turnover`、`view_arb_hit_ratio`。

## 运行命令
- `python -m polaris.cli arb-run --mode paper_live`
- `python -m polaris.cli arb-run --mode shadow --once`
- `python -m polaris.cli arb-replay --start 2026-02-10T00:00:00+00:00 --end 2026-02-10T06:00:00+00:00`
- `python -m polaris.cli arb-report --group-by strategy,mode,source`
- `python -m polaris.cli arb-export --table arb_trade_result --format csv --since-hours 24`

## 风控默认值（$10 起步）
- 最小下单金额: $1
- 单笔风险上限: $2
- 最大在途敞口: $6
- 日止损: $0.5
- 连续失败阈值: 2

## 参数进化
- 每日生成候选参数。
- 评分由历史回放 + 最近 24h paper_live 组合。
- 满足“收益提升且风险不恶化”才晋升 active。
