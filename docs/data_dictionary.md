# Polaris Data Dictionary

| Name | Type | Description |
|---|---|---|
| `dim_account` | Dimension | Account identity, profile metadata, source sync timestamps. |
| `dim_tracking_window` | Dimension | XTracker windows used for count settlement. |
| `dim_market` | Dimension | Normalized Polymarket market metadata. |
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
