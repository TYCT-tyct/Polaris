-- Module2: 实验运行与评分框架（并行参数实验 + 自动晋级）
create table if not exists arb_experiment_run (
    experiment_run_id text primary key,
    mode text not null,
    run_tag text not null,
    source_code text not null,
    profile text not null default 'unknown',
    strategy_set text not null default 'unknown',
    scope text not null default 'shared',
    variant text not null default 'base',
    bankroll_usd numeric(20, 8) not null default 50.0,
    status text not null default 'running',
    params jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    started_at timestamptz not null default now(),
    ended_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists idx_arb_experiment_run_unique
    on arb_experiment_run(mode, run_tag, source_code);

create index if not exists idx_arb_experiment_run_core
    on arb_experiment_run(updated_at desc, mode, run_tag, profile, strategy_set, scope);

create table if not exists arb_experiment_metric (
    metric_id bigserial primary key,
    experiment_run_id text not null references arb_experiment_run(experiment_run_id) on delete cascade,
    mode text not null,
    run_tag text not null,
    source_code text not null,
    profile text not null default 'unknown',
    strategy_set text not null default 'unknown',
    scope text not null default 'shared',
    variant text not null default 'base',

    since_hours integer not null,
    signals_found integer not null default 0,
    signals_executed integer not null default 0,
    signals_rejected integer not null default 0,
    signals_expired integer not null default 0,
    trades integer not null default 0,
    wins integer not null default 0,
    losses integer not null default 0,

    realized_net_pnl_usd numeric(20, 8) not null default 0,
    mark_to_book_net_pnl_usd numeric(20, 8) not null default 0,
    expected_net_pnl_usd numeric(20, 8) not null default 0,
    evaluation_net_pnl_usd numeric(20, 8) not null default 0,
    pnl_gap_vs_expected_usd numeric(20, 8) not null default 0,
    turnover_usd numeric(20, 8) not null default 0,
    max_drawdown_usd numeric(20, 8) not null default 0,
    max_drawdown_pct numeric(20, 8) not null default 0,

    execution_rate numeric(20, 8) not null default 0,
    reject_rate numeric(20, 8) not null default 0,
    win_rate numeric(20, 8) not null default 0,
    system_error_rate numeric(20, 8) not null default 0,
    resource_penalty numeric(20, 8) not null default 0,

    score_total numeric(20, 8) not null default 0,
    score_breakdown jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_experiment_metric_core
    on arb_experiment_metric(created_at desc, mode, run_tag, profile, strategy_set, scope, score_total desc);
