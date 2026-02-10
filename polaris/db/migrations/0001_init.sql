create extension if not exists pgcrypto;

create table if not exists dim_account (
    account_id text primary key,
    handle text not null unique,
    platform_id text,
    platform text not null default 'X',
    display_name text,
    avatar_url text,
    bio text,
    verified boolean,
    post_count bigint,
    last_sync timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_tracking_window (
    tracking_id text not null,
    account_id text not null references dim_account(account_id) on delete cascade,
    title text not null,
    start_date timestamptz not null,
    end_date timestamptz not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tracking_id, account_id)
);

create table if not exists fact_tweet_metric_daily (
    account_id text not null references dim_account(account_id) on delete cascade,
    tracking_id text not null,
    metric_date date not null,
    tweet_count integer not null,
    cumulative_count integer not null,
    source_metric_id text,
    captured_at timestamptz not null default now(),
    primary key (account_id, tracking_id, metric_date)
);

create table if not exists fact_tweet_post (
    platform_post_id text primary key,
    account_id text not null references dim_account(account_id) on delete cascade,
    source_post_id text not null unique,
    content text not null,
    posted_at timestamptz not null,
    imported_at timestamptz,
    is_reply boolean not null default false,
    is_retweet boolean not null default false,
    has_url boolean not null default false,
    language_guess text not null default 'unknown',
    char_len integer not null default 0,
    content_hash text not null,
    raw_payload jsonb not null,
    captured_at timestamptz not null default now()
);

create index if not exists idx_fact_tweet_post_account_posted
on fact_tweet_post(account_id, posted_at desc);

create table if not exists dim_market (
    market_id text primary key,
    gamma_market_id bigint,
    condition_id text unique,
    question text not null,
    slug text not null unique,
    event_slug text,
    category text,
    end_date timestamptz,
    start_date timestamptz,
    neg_risk boolean not null default false,
    active boolean not null default false,
    closed boolean not null default false,
    archived boolean not null default false,
    spread numeric(12, 6),
    liquidity numeric(20, 6),
    volume numeric(20, 6),
    updated_from_source_at timestamptz,
    captured_at timestamptz not null default now()
);

create index if not exists idx_dim_market_active_end
on dim_market(active, end_date);

create table if not exists dim_token (
    token_id text primary key,
    market_id text not null references dim_market(market_id) on delete cascade,
    outcome_label text not null,
    outcome_side text not null,
    tick_size numeric(12, 6),
    min_order_size numeric(20, 6),
    captured_at timestamptz not null default now(),
    unique (market_id, outcome_label, outcome_side)
);

create index if not exists idx_dim_token_market
on dim_token(market_id, outcome_label);

create table if not exists bridge_market_tracking (
    market_id text not null references dim_market(market_id) on delete cascade,
    tracking_id text not null,
    account_id text not null,
    match_score numeric(10, 6) not null,
    match_reason text not null,
    mapped_at timestamptz not null default now(),
    primary key (market_id, tracking_id)
);

create table if not exists fact_market_state_snapshot (
    id bigserial primary key,
    market_id text not null references dim_market(market_id) on delete cascade,
    active boolean not null,
    closed boolean not null,
    archived boolean not null,
    spread numeric(12, 6),
    liquidity numeric(20, 6),
    volume numeric(20, 6),
    captured_at timestamptz not null default now()
);

create table if not exists fact_quote_top_raw (
    id bigserial not null,
    token_id text not null references dim_token(token_id) on delete cascade,
    market_id text not null references dim_market(market_id) on delete cascade,
    best_bid numeric(12, 6),
    best_ask numeric(12, 6),
    mid numeric(12, 6),
    spread numeric(12, 6),
    last_trade_price numeric(12, 6),
    source_ts timestamptz,
    captured_at timestamptz not null default now(),
    primary key (id, captured_at)
) partition by range (captured_at);

create table if not exists fact_quote_depth_raw (
    id bigserial not null,
    token_id text not null references dim_token(token_id) on delete cascade,
    market_id text not null references dim_market(market_id) on delete cascade,
    bid_depth_1pct numeric(20, 6) not null default 0,
    bid_depth_2pct numeric(20, 6) not null default 0,
    bid_depth_5pct numeric(20, 6) not null default 0,
    ask_depth_1pct numeric(20, 6) not null default 0,
    ask_depth_2pct numeric(20, 6) not null default 0,
    ask_depth_5pct numeric(20, 6) not null default 0,
    imbalance numeric(12, 6),
    source_ts timestamptz,
    captured_at timestamptz not null default now(),
    primary key (id, captured_at)
) partition by range (captured_at);

create table if not exists fact_orderbook_l2_raw (
    id bigserial not null,
    token_id text not null references dim_token(token_id) on delete cascade,
    market_id text not null references dim_market(market_id) on delete cascade,
    side text not null,
    price numeric(12, 6) not null,
    size numeric(20, 6) not null,
    level_index integer not null,
    source_ts timestamptz,
    captured_at timestamptz not null default now(),
    primary key (id, captured_at)
) partition by range (captured_at);

create table if not exists fact_quote_1m (
    token_id text not null references dim_token(token_id) on delete cascade,
    market_id text not null references dim_market(market_id) on delete cascade,
    bucket_minute timestamptz not null,
    open numeric(12, 6),
    high numeric(12, 6),
    low numeric(12, 6),
    close numeric(12, 6),
    avg_spread numeric(12, 6),
    min_spread numeric(12, 6),
    max_spread numeric(12, 6),
    sample_count integer not null,
    updated_at timestamptz not null default now(),
    primary key (token_id, bucket_minute)
);

create index if not exists idx_fact_quote_1m_market_time
on fact_quote_1m(market_id, bucket_minute desc);

create table if not exists fact_settlement (
    market_id text primary key references dim_market(market_id) on delete cascade,
    settled_at timestamptz,
    winning_outcome text,
    winning_token_id text,
    final_tweet_count integer,
    payload jsonb not null default '{}'::jsonb
);

create table if not exists ops_collector_run (
    run_id uuid primary key default gen_random_uuid(),
    job_name text not null,
    source text not null,
    status text not null,
    status_code integer,
    rows_written integer not null default 0,
    latency_ms integer not null default 0,
    error_code text,
    error_message text,
    started_at timestamptz not null,
    finished_at timestamptz not null,
    metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_ops_collector_run_time
on ops_collector_run(started_at desc);

create table if not exists ops_api_health_minute (
    minute_bucket timestamptz not null,
    source text not null,
    status text not null,
    runs integer not null,
    error_runs integer not null,
    avg_latency_ms numeric(12, 4),
    p95_latency_ms numeric(12, 4),
    created_at timestamptz not null default now(),
    primary key (minute_bucket, source, status)
);

create table if not exists ops_cursor (
    cursor_key text primary key,
    cursor_value text not null,
    meta jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

