# Polaris Data Dictionary

## Dimensions
- `dim_account`: account identity, profile metadata, source sync timestamps.
- `dim_tracking_window`: XTracker windows used for count settlement.
- `dim_market`: normalized Polymarket market metadata.
- `dim_token`: token/outcome mapping for CLOB.

## Bridges
- `bridge_market_tracking`: best-match relationship between market windows and tracking windows.

## Tweet Facts
- `fact_tweet_metric_daily`: daily count and cumulative count per tracking window.
- `fact_tweet_post`: normalized post content and derived text features.

## Market Facts
- `fact_market_state_snapshot`: status snapshots from Gamma.
- `fact_quote_top_raw`: top-of-book snapshots from CLOB.
- `fact_quote_depth_raw`: depth summaries from CLOB.
- `fact_orderbook_l2_raw`: L2 orderbook levels (optional, enabled by config).
- `fact_quote_1m`: minute OHLC and spread aggregates.
- `fact_settlement`: settlement-compatible schema for downstream modules.

## Ops Facts
- `ops_collector_run`: per-job execution logs.
- `ops_api_health_minute`: latency/error rollups per minute.
- `ops_cursor`: ingestion checkpoints (hashes, IDs, progress values).

## Read-Optimized Views
- `view_quote_latest`: latest quote per token for low-latency readers.

