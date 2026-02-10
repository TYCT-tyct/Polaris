# Polaris Module 1 DataHarvester Design

## Goal
Provide a production-grade data ingestion layer for Elon tweet prediction markets on Polymarket. Module 1 is deliberately strategy-free; it only collects, normalizes, and stores truth data.

## Data Sources
- XTracker:
  - `/api/users/{handle}`
  - `/api/users/{handle}/trackings`
  - `/api/metrics/{userId}`
  - `/api/users/{handle}/posts`
- Gamma:
  - `/markets` (paged, descending by id)
- CLOB:
  - `/book?token_id=...`

## Core Pipeline
1. Discover target markets from Gamma and upsert market/token dimensions.
2. Sync X account and tracking windows.
3. Pull daily metrics for each active tracking window.
4. Pull post content from XTracker and apply incremental insertion:
   - hash payload (`sha256`)
   - skip parse if hash unchanged
   - if changed, insert only new posts above `last_post_id`
5. Collect orderbook snapshots per active token:
   - top-of-book
   - depth summary (1/2/5% bands)
   - optional L2 levels
6. Build 1-minute aggregates and latest-quote view.
7. Persist run-level health metrics.

## Cost and Throughput Constraints
- No official paid X API.
- XTracker post endpoint currently returns full list; incremental hash + cursor gate avoids repeated heavy writes.
- Default source rate limits:
  - XTracker: 1 req/s burst 2
  - Gamma: 2 req/s burst 4
  - CLOB: 5 req/s burst 8

## Reliability Model
- Retries for timeout/network/429/5xx with exponential backoff and jitter.
- Collector failure is isolated per job.
- Every collector run writes an audit row (`ops_collector_run`).
- Health rollups are materialized per minute (`ops_api_health_minute`).

## Data Retention
- Raw high-frequency tables: 14 days.
- Minute aggregates: long-term retention.

## Commands
- `python -m polaris.cli migrate`
- `python -m polaris.cli harvest-once --handle elonmusk`
- `python -m polaris.cli run --handle elonmusk`
- `python -m polaris.cli backfill --handle elonmusk --start YYYY-MM-DD --end YYYY-MM-DD`
- `python -m polaris.cli doctor`

