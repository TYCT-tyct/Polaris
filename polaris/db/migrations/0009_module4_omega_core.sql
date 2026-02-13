alter table dim_market
    add column if not exists description text;

create table if not exists dim_market_rule_version (
    rule_version_id uuid primary key default gen_random_uuid(),
    market_id text not null references dim_market(market_id) on delete cascade,
    rule_hash text not null,
    rule_text text not null default '',
    parsed_rule jsonb not null default '{}'::jsonb,
    valid_from timestamptz not null default now(),
    valid_to timestamptz,
    created_at timestamptz not null default now(),
    unique (market_id, rule_hash)
);

create index if not exists idx_dim_market_rule_version_market_open
    on dim_market_rule_version(market_id, valid_to, created_at desc);

create table if not exists fact_tweet_metric_1m (
    account_id text not null references dim_account(account_id) on delete cascade,
    tracking_id text not null,
    minute_bucket timestamptz not null,
    cumulative_count_est integer not null,
    cumulative_count_metric integer,
    cumulative_count_posts integer not null,
    source_confidence numeric(12, 6) not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    captured_at timestamptz not null default now(),
    primary key (account_id, tracking_id, minute_bucket)
);

create index if not exists idx_fact_tweet_metric_1m_tracking_time
    on fact_tweet_metric_1m(tracking_id, minute_bucket desc);

create table if not exists m4_posterior_snapshot (
    snapshot_id bigserial primary key,
    run_tag text not null,
    mode text not null default 'paper_live',
    source_code text not null default 'module4',
    window_code text not null,
    market_id text not null references dim_market(market_id) on delete cascade,
    tracking_id text,
    account_id text references dim_account(account_id) on delete set null,
    rule_version_id uuid references dim_market_rule_version(rule_version_id) on delete set null,
    progress numeric(12, 6) not null default 0,
    progress_effective numeric(12, 6) not null default 0,
    observed_count integer not null default 0,
    expected_count numeric(20, 6) not null default 0,
    entropy numeric(20, 8) not null default 0,
    prior_reliability numeric(12, 6) not null default 0,
    prior_pmf jsonb not null default '{}'::jsonb,
    posterior_pmf jsonb not null default '{}'::jsonb,
    quantiles jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_m4_posterior_snapshot_core
    on m4_posterior_snapshot(created_at desc, run_tag, mode, source_code, window_code, market_id);

create table if not exists m4_decision_log (
    decision_id uuid primary key default gen_random_uuid(),
    run_tag text not null,
    mode text not null default 'paper_live',
    source_code text not null default 'module4',
    window_code text not null default 'fused',
    market_id text not null references dim_market(market_id) on delete cascade,
    tracking_id text,
    account_id text references dim_account(account_id) on delete set null,
    rule_version_id uuid references dim_market_rule_version(rule_version_id) on delete set null,
    action text not null,
    reason_codes text[] not null default '{}'::text[],
    prior_bucket text,
    target_bucket text,
    target_prob_prior numeric(20, 8),
    target_prob_posterior numeric(20, 8),
    edge_expected numeric(20, 8) not null default 0,
    es95_loss numeric(20, 8) not null default 0,
    net_edge numeric(20, 8) not null default 0,
    size_usd numeric(20, 8) not null default 0,
    fee_est_usd numeric(20, 8) not null default 0,
    slippage_est_usd numeric(20, 8) not null default 0,
    realized_net_pnl_usd numeric(20, 8),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_m4_decision_log_core
    on m4_decision_log(created_at desc, run_tag, mode, source_code, window_code, market_id);

create table if not exists m4_model_score (
    score_id bigserial primary key,
    run_tag text not null,
    mode text not null default 'paper_live',
    source_code text not null default 'module4',
    window_code text not null default 'fused',
    since_hours integer not null,
    markets_scored integer not null default 0,
    log_score numeric(20, 8),
    ece numeric(20, 8),
    brier numeric(20, 8),
    realized_net_pnl_usd numeric(20, 8) not null default 0,
    max_drawdown_usd numeric(20, 8) not null default 0,
    score_total numeric(20, 8) not null default 0,
    score_breakdown jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_m4_model_score_core
    on m4_model_score(created_at desc, run_tag, mode, source_code, window_code, score_total desc);

create table if not exists m4_evidence_log (
    evidence_id uuid primary key default gen_random_uuid(),
    run_tag text not null,
    mode text not null default 'paper_live',
    source_code text not null default 'module4',
    market_id text references dim_market(market_id) on delete cascade,
    tracking_id text,
    account_id text references dim_account(account_id) on delete set null,
    agent_name text not null default 'evidence_agent',
    calls_used integer not null default 0,
    timed_out boolean not null default false,
    degraded boolean not null default false,
    confidence numeric(12, 6) not null default 0,
    uncertainty_delta numeric(12, 6) not null default 0,
    event_tags text[] not null default '{}'::text[],
    sources jsonb not null default '[]'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_m4_evidence_log_core
    on m4_evidence_log(created_at desc, run_tag, mode, source_code, market_id);
