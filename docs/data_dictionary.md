# Polaris Data Dictionary

| Name | Type | Description |
|---|---|---|
| `dim_account` | Dimension | Account identity, profile metadata, source sync timestamps. |
| `dim_tracking_window` | Dimension | XTracker windows used for count settlement. |
| `dim_market` | Dimension | Normalized Polymarket market metadata. |
| `dim_event` | Dimension | Event-level metadata used by negRisk grouping and strategy aggregation. |
| `dim_token` | Dimension | Token/outcome mapping for CLOB books. |
| `bridge_market_tracking` | Bridge | Best-match relationship between market windows and tracking windows. |
| `fact_tweet_metric_daily` | Fact | Daily count and cumulative count per tracking window. |
| `fact_tweet_post` | Fact | Normalized post content and derived text features. |
| `fact_market_state_snapshot` | Fact | Market status snapshots from Gamma. |
| `fact_quote_top_raw` | Fact (raw) | Top-of-book snapshots from CLOB. |
| `fact_quote_depth_raw` | Fact (raw) | Depth summary snapshots from CLOB. |
| `fact_orderbook_l2_raw` | Fact (raw) | L2 orderbook levels from CLOB. |
| `fact_quote_1m` | Fact (aggregate) | Minute OHLC and spread aggregates. |
| `fact_settlement` | Fact | Settlement-compatible schema for downstream modules. |
| `ops_collector_run` | Ops | Per-job execution logs. |
| `ops_api_health_minute` | Ops | Per-minute latency/error rollups. |
| `ops_cursor` | Ops | Ingestion checkpoints (hashes, IDs, progress values). |
| `view_quote_latest` | View | Latest quote per token for low-latency readers. |
| `arb_signal` | Module2 Fact | Opportunity signals produced by strategies A/B/C/F/G. |
| `arb_order_intent` | Module2 Fact | Planned order legs before execution. |
| `arb_order_event` | Module2 Fact | Order lifecycle events (submit/fill/reject/cancel/error). |
| `arb_fill` | Module2 Fact | Fill-level execution records. |
| `arb_position_lot` | Module2 Fact | Position lifecycle by lot. |
| `arb_cash_ledger` | Module2 Fact | Cash flow ledger for PnL tracking. |
| `arb_trade_result` | Module2 Fact | Trade-level PnL and turnover summary. |
| `arb_risk_event` | Module2 Ops | Risk gate and circuit-breaker events. |
| `arb_replay_run` | Module2 Ops | Replay job metadata and execution window. |
| `arb_replay_metric` | Module2 Ops | Replay performance metrics by strategy. |
| `arb_param_snapshot` | Module2 Ops | Parameter candidates, active versions, and scores. |
| `view_arb_pnl_daily` | Module2 View | Daily PnL aggregate by strategy/mode/source. |
| `view_arb_by_strategy_mode_source` | Module2 View | Aggregated win/loss/pnl/turnover breakdown. |
| `view_arb_turnover` | Module2 View | Hourly turnover summary. |
| `view_arb_hit_ratio` | Module2 View | Hit ratio summary by strategy/mode/source. |
