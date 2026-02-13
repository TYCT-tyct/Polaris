from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from polaris.cli import _arb_cleanup_apply, _arb_cleanup_counts


@pytest.mark.asyncio
async def test_arb_clean_counts_and_apply_support_replay_tables(db) -> None:
    await db.execute(
        """
        truncate table
            arb_experiment_metric,
            arb_experiment_run,
            arb_order_event,
            arb_fill,
            arb_order_intent,
            arb_position_lot,
            arb_cash_ledger,
            arb_risk_event,
            arb_trade_result,
            arb_signal,
            arb_replay_metric,
            arb_replay_run,
            arb_param_snapshot
        restart identity cascade
        """
    )

    signal_id = "00000000-0000-0000-0000-000000000101"
    intent_id = "00000000-0000-0000-0000-000000000102"
    replay_run_id = "00000000-0000-0000-0000-000000000103"
    experiment_run_id = "00000000-0000-0000-0000-000000000108"
    now = datetime.now(tz=UTC)

    await db.execute(
        """
        insert into arb_signal(
            signal_id, mode, strategy_code, source_code, event_id, market_ids, token_ids,
            edge_pct, expected_pnl_usd, ttl_ms, features, status, decision_note, created_at
        )
        values (
            %s, 'paper_live', 'A', 'paper', 'event-1', '{market-1}', '{token-1}',
            0.01, 0.1, 10000, %s::jsonb, 'new', 'unit', %s
        )
        """,
        (signal_id, json.dumps({"run_tag": "tag1"}), now),
    )
    await db.execute(
        """
        insert into arb_order_intent(
            intent_id, signal_id, mode, strategy_code, source_code, order_index, market_id,
            token_id, side, order_type, limit_price, shares, notional_usd, status, payload, created_at
        )
        values (
            %s, %s, 'paper_live', 'A', 'paper', 0, 'market-1', 'token-1',
            'BUY', 'FAK', 0.5, 2, 1.0, 'created', '{}'::jsonb, %s
        )
        """,
        (intent_id, signal_id, now),
    )
    await db.execute(
        """
        insert into arb_order_event(intent_id, event_type, status_code, message, payload, created_at)
        values (%s, 'submit', 200, 'ok', '{}'::jsonb, %s)
        """,
        (intent_id, now),
    )
    await db.execute(
        """
        insert into arb_fill(intent_id, market_id, token_id, side, fill_price, fill_size, fill_notional_usd, fee_usd, created_at)
        values (%s, 'market-1', 'token-1', 'BUY', 0.5, 2, 1.0, 0, %s)
        """,
        (intent_id, now),
    )
    await db.execute(
        """
        insert into arb_trade_result(
            trade_id, signal_id, mode, strategy_code, source_code, status,
            gross_pnl_usd, fees_usd, slippage_usd, net_pnl_usd, capital_used_usd,
            hold_minutes, opened_at, closed_at, metadata, created_at
        )
        values (
            '00000000-0000-0000-0000-000000000104', %s, 'paper_live', 'A', 'paper', 'filled',
            0.1, 0, 0, 0.1, 1.0, 1, %s, %s, %s::jsonb, %s
        )
        """,
        (signal_id, now, now, json.dumps({"run_tag": "tag1"}), now),
    )
    await db.execute(
        """
        insert into arb_cash_ledger(
            mode, strategy_code, source_code, entry_type, amount_usd,
            balance_before_usd, balance_after_usd, ref_signal_id, payload, created_at
        )
        values ('paper_live', 'A', 'paper', 'trade_net_pnl', 0.1, 50, 50.1, %s, %s::jsonb, %s)
        """,
        (signal_id, json.dumps({"run_tag": "tag1"}), now),
    )
    await db.execute(
        """
        insert into arb_risk_event(
            risk_event_id, mode, strategy_code, source_code, event_type, severity, reason, payload, created_at
        )
        values (
            '00000000-0000-0000-0000-000000000105',
            'paper_live', 'A', 'paper', 'risk_gate', 'warn', 'single_trade_risk_exceeded', %s::jsonb, %s
        )
        """,
        (json.dumps({"run_tag": "tag1"}), now),
    )
    await db.execute(
        """
        insert into arb_position_lot(
            position_id, mode, strategy_code, source_code, run_tag, market_id, token_id, side,
            open_price, open_size, open_notional_usd, remaining_size, status, opened_at, closed_at, close_price, realized_pnl_usd
        )
        values (
            '00000000-0000-0000-0000-000000000106',
            'paper_live', 'A', 'paper', 'tag1', 'market-1', 'token-1', 'BUY',
            0.5, 2, 1.0, 0, 'closed', %s, %s, 0.55, 0.1
        )
        """,
        (now, now),
    )
    await db.execute(
        """
        insert into arb_replay_run(
            replay_run_id, mode, status, window_start, window_end, params_version, sample_count, metadata, started_at, finished_at
        )
        values (
            %s, 'paper_replay', 'done', %s, %s, 'v1', 10, %s::jsonb, %s, %s
        )
        """,
        (replay_run_id, now, now, json.dumps({"source_code": "paper", "run_tag": "tag1"}), now, now),
    )
    await db.execute(
        """
        insert into arb_replay_metric(
            replay_run_id, strategy_code, signals, trades, wins, losses,
            gross_pnl_usd, net_pnl_usd, max_drawdown_usd, turnover_usd, created_at
        )
        values (%s, 'A', 1, 1, 1, 0, 0.1, 0.1, 0, 1.0, %s)
        """,
        (replay_run_id, now),
    )
    await db.execute(
        """
        insert into arb_experiment_run(
            experiment_run_id, mode, run_tag, source_code, profile, strategy_set, scope, variant,
            bankroll_usd, status, params, metadata, started_at, created_at, updated_at
        )
        values (
            %s, 'paper_live', 'tag1', 'paper', 'balanced_50', 'ABCFG', 'shared', 'base',
            50, 'running', '{}'::jsonb, '{}'::jsonb, %s, %s, %s
        )
        """,
        (experiment_run_id, now, now, now),
    )
    await db.execute(
        """
        insert into arb_experiment_metric(
            experiment_run_id, mode, run_tag, source_code, profile, strategy_set, scope, variant,
            since_hours, signals_found, signals_executed, signals_rejected, signals_expired,
            trades, wins, losses, realized_net_pnl_usd, mark_to_book_net_pnl_usd,
            expected_net_pnl_usd, evaluation_net_pnl_usd, pnl_gap_vs_expected_usd,
            turnover_usd, max_drawdown_usd, max_drawdown_pct, execution_rate, reject_rate, win_rate,
            system_error_rate, resource_penalty, score_total, score_breakdown, metadata, created_at
        )
        values (
            %s, 'paper_live', 'tag1', 'paper', 'balanced_50', 'ABCFG', 'shared', 'base',
            8, 10, 3, 7, 0, 3, 2, 1, 0.2, 0.1, 0.3, 0.25, -0.05, 12, 0.5, 0.01,
            0.3, 0.7, 0.66, 0.0, 0.0, 0.15, '{}'::jsonb, '{}'::jsonb, %s
        )
        """,
        (experiment_run_id, now),
    )
    await db.execute(
        """
        insert into arb_param_snapshot(
            param_snapshot_id, strategy_scope, version, status, params, score_total, score_breakdown,
            source_paper_window_start, source_paper_window_end, created_at
        )
        values (
            '00000000-0000-0000-0000-000000000107',
            'module2', 'v-unit', 'candidate', '{}'::jsonb, 1.0, %s::jsonb, %s, %s, %s
        )
        """,
        (json.dumps({"run_tag": "tag1"}), now, now, now),
    )

    counts = await _arb_cleanup_counts(
        db,
        mode_filter=None,
        source_filter="paper",
        run_tag_filter="tag1",
        since_start=None,
        include_position_lot=True,
        include_param_snapshot=True,
    )
    assert counts["arb_signal"] == 1
    assert counts["arb_order_intent"] == 1
    assert counts["arb_order_event"] == 1
    assert counts["arb_fill"] == 1
    assert counts["arb_trade_result"] == 1
    assert counts["arb_cash_ledger"] == 1
    assert counts["arb_risk_event"] == 1
    assert counts["arb_experiment_run"] == 1
    assert counts["arb_experiment_metric"] == 1
    assert counts["arb_position_lot"] == 1
    assert counts["arb_replay_run"] == 1
    assert counts["arb_replay_metric"] == 1
    assert counts["arb_param_snapshot"] == 1

    deleted = await _arb_cleanup_apply(
        db,
        mode_filter=None,
        source_filter="paper",
        run_tag_filter="tag1",
        since_start=None,
        include_position_lot=True,
        include_param_snapshot=True,
    )
    assert deleted["arb_signal"] == 1
    assert deleted["arb_order_intent"] == 1
    assert deleted["arb_order_event"] == 1
    assert deleted["arb_fill"] == 1
    assert deleted["arb_trade_result"] == 1
    assert deleted["arb_cash_ledger"] == 1
    assert deleted["arb_risk_event"] == 1
    assert deleted["arb_experiment_run"] == 1
    assert deleted["arb_experiment_metric"] == 1
    assert deleted["arb_position_lot"] == 1
    assert deleted["arb_replay_run"] == 1
    assert deleted["arb_replay_metric"] == 1
    assert deleted["arb_param_snapshot"] == 1

    remain = await _arb_cleanup_counts(
        db,
        mode_filter=None,
        source_filter="paper",
        run_tag_filter="tag1",
        since_start=None,
        include_position_lot=True,
        include_param_snapshot=True,
    )
    assert all(v == 0 for v in remain.values())
