alter table dim_market
    add column if not exists event_id text,
    add column if not exists neg_risk_augmented boolean not null default false,
    add column if not exists resolution_source text;

alter table dim_token
    add column if not exists outcome_index integer not null default 0,
    add column if not exists is_other_outcome boolean not null default false,
    add column if not exists is_placeholder_outcome boolean not null default false;

create index if not exists idx_dim_market_event_id on dim_market(event_id);
create index if not exists idx_dim_market_neg_risk_active on dim_market(neg_risk, active, closed);
create index if not exists idx_dim_token_market_idx on dim_token(market_id, outcome_index);

create table if not exists dim_event (
    event_id text primary key,
    event_slug text,
    event_ticker text,
    title text,
    category text,
    active boolean not null default false,
    closed boolean not null default false,
    archived boolean not null default false,
    start_date timestamptz,
    end_date timestamptz,
    captured_at timestamptz not null default now()
);

create table if not exists arb_signal (
    signal_id uuid primary key default gen_random_uuid(),
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    event_id text,
    market_ids text[] not null default '{}'::text[],
    token_ids text[] not null default '{}'::text[],
    edge_pct numeric(12, 6),
    expected_pnl_usd numeric(20, 6),
    ttl_ms integer not null default 10000,
    features jsonb not null default '{}'::jsonb,
    status text not null default 'new',
    decision_note text,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_signal_created on arb_signal(created_at desc);
create index if not exists idx_arb_signal_strategy on arb_signal(strategy_code, mode, source_code);

create table if not exists arb_order_intent (
    intent_id uuid primary key default gen_random_uuid(),
    signal_id uuid references arb_signal(signal_id) on delete cascade,
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    order_index integer not null default 0,
    market_id text,
    token_id text,
    side text not null,
    order_type text not null,
    limit_price numeric(12, 6),
    shares numeric(20, 8),
    notional_usd numeric(20, 8),
    status text not null default 'created',
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_order_intent_signal on arb_order_intent(signal_id);
create index if not exists idx_arb_order_intent_mode on arb_order_intent(mode, strategy_code, created_at desc);

create table if not exists arb_order_event (
    order_event_id uuid primary key default gen_random_uuid(),
    intent_id uuid references arb_order_intent(intent_id) on delete cascade,
    event_type text not null,
    status_code integer,
    message text,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_order_event_intent on arb_order_event(intent_id, created_at);

create table if not exists arb_fill (
    fill_id bigserial primary key,
    intent_id uuid references arb_order_intent(intent_id) on delete cascade,
    market_id text,
    token_id text,
    side text not null,
    fill_price numeric(12, 6) not null,
    fill_size numeric(20, 8) not null,
    fill_notional_usd numeric(20, 8) not null,
    fee_usd numeric(20, 8) not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_fill_intent on arb_fill(intent_id, created_at);

create table if not exists arb_position_lot (
    position_id uuid primary key default gen_random_uuid(),
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    market_id text,
    token_id text,
    side text not null,
    open_intent_id uuid references arb_order_intent(intent_id) on delete set null,
    open_price numeric(12, 6),
    open_size numeric(20, 8),
    open_notional_usd numeric(20, 8),
    remaining_size numeric(20, 8),
    status text not null default 'open',
    opened_at timestamptz not null default now(),
    closed_at timestamptz,
    close_price numeric(12, 6),
    realized_pnl_usd numeric(20, 8)
);

create index if not exists idx_arb_position_lot_open on arb_position_lot(status, opened_at desc);

create table if not exists arb_cash_ledger (
    ledger_id bigserial primary key,
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    entry_type text not null,
    amount_usd numeric(20, 8) not null,
    balance_before_usd numeric(20, 8),
    balance_after_usd numeric(20, 8),
    ref_signal_id uuid references arb_signal(signal_id) on delete set null,
    ref_intent_id uuid references arb_order_intent(intent_id) on delete set null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_cash_ledger_mode on arb_cash_ledger(mode, strategy_code, created_at desc);

create table if not exists arb_trade_result (
    trade_id uuid primary key default gen_random_uuid(),
    signal_id uuid references arb_signal(signal_id) on delete set null,
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    status text not null,
    gross_pnl_usd numeric(20, 8) not null default 0,
    fees_usd numeric(20, 8) not null default 0,
    slippage_usd numeric(20, 8) not null default 0,
    net_pnl_usd numeric(20, 8) not null default 0,
    capital_used_usd numeric(20, 8) not null default 0,
    hold_minutes numeric(20, 8),
    opened_at timestamptz,
    closed_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_trade_result_mode on arb_trade_result(mode, strategy_code, source_code, created_at desc);
create index if not exists idx_arb_trade_result_signal on arb_trade_result(signal_id);

create table if not exists arb_risk_event (
    risk_event_id uuid primary key default gen_random_uuid(),
    mode text not null,
    strategy_code text,
    source_code text not null default 'polymarket',
    event_type text not null,
    severity text not null,
    reason text not null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_risk_event_mode on arb_risk_event(mode, created_at desc);

create table if not exists arb_replay_run (
    replay_run_id uuid primary key default gen_random_uuid(),
    mode text not null default 'paper_replay',
    status text not null default 'running',
    window_start timestamptz not null,
    window_end timestamptz not null,
    params_version text,
    sample_count integer not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    started_at timestamptz not null default now(),
    finished_at timestamptz
);

create table if not exists arb_replay_metric (
    replay_metric_id bigserial primary key,
    replay_run_id uuid not null references arb_replay_run(replay_run_id) on delete cascade,
    strategy_code text not null,
    signals integer not null default 0,
    trades integer not null default 0,
    wins integer not null default 0,
    losses integer not null default 0,
    gross_pnl_usd numeric(20, 8) not null default 0,
    net_pnl_usd numeric(20, 8) not null default 0,
    max_drawdown_usd numeric(20, 8) not null default 0,
    turnover_usd numeric(20, 8) not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_replay_metric_run on arb_replay_metric(replay_run_id, strategy_code);

create table if not exists arb_param_snapshot (
    param_snapshot_id uuid primary key default gen_random_uuid(),
    strategy_scope text not null default 'module2',
    version text not null,
    status text not null default 'candidate',
    params jsonb not null,
    score_total numeric(20, 8),
    score_breakdown jsonb not null default '{}'::jsonb,
    source_replay_run_id uuid references arb_replay_run(replay_run_id) on delete set null,
    source_paper_window_start timestamptz,
    source_paper_window_end timestamptz,
    created_at timestamptz not null default now(),
    activated_at timestamptz,
    deactivated_at timestamptz
);

create unique index if not exists uq_arb_param_snapshot_version on arb_param_snapshot(version);
