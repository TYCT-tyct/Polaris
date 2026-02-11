from __future__ import annotations

import pytest

from polaris.arb.reporting import ArbReporter


@pytest.mark.asyncio
async def test_arb_summary_includes_strategy_totals_and_latency(db) -> None:
    reporter = ArbReporter(db)
    run_tag = "unit-report"

    signal_a = "11111111-1111-1111-1111-111111111111"
    signal_f = "22222222-2222-2222-2222-222222222222"
    signal_g = "33333333-3333-3333-3333-333333333333"
    signal_shadow = "44444444-4444-4444-4444-444444444444"
    intent_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    intent_f = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    await db.execute(
        """
        insert into arb_signal(
            signal_id, mode, strategy_code, source_code, event_id, market_ids, token_ids,
            edge_pct, expected_pnl_usd, ttl_ms, features, status, decision_note, created_at
        ) values (
            %s, 'paper_live', 'A', 'polymarket', 'event-a', '{m-a}', '{t-a}',
            0.03, 0.05, 10000, %s::jsonb, 'executed', 'a ok', now() - interval '90 minutes'
        ), (
            %s, 'paper_live', 'F', 'polymarket', 'event-f', '{m-f}', '{t-f}',
            0.02, 0.02, 10000, %s::jsonb, 'executed', 'f ok', now() - interval '80 minutes'
        ), (
            %s, 'paper_live', 'G', 'polymarket', 'event-g', '{m-g}', '{t-g}',
            0.01, 0.01, 10000, %s::jsonb, 'rejected', 'g rejected', now() - interval '70 minutes'
        ), (
            %s, 'shadow', 'A', 'polymarket', 'event-shadow', '{m-s}', '{t-s}',
            0.01, 0.01, 10000, %s::jsonb, 'new', 'shadow only', now() - interval '60 minutes'
        )
        """,
        (
            signal_a,
            '{"run_tag":"unit-report"}',
            signal_f,
            '{"run_tag":"unit-report"}',
            signal_g,
            '{"run_tag":"unit-report"}',
            signal_shadow,
            '{"run_tag":"unit-report"}',
        ),
    )

    await db.execute(
        """
        insert into arb_order_intent(
            intent_id, signal_id, mode, strategy_code, source_code, order_index,
            market_id, token_id, side, order_type, limit_price, shares, notional_usd,
            status, payload, created_at
        ) values (
            %s, %s, 'paper_live', 'A', 'polymarket', 0,
            'm-a', 't-a', 'BUY', 'LIMIT', 0.50, 2, 1.00,
            'accepted', '{}'::jsonb, now() - interval '89 minutes'
        ), (
            %s, %s, 'paper_live', 'F', 'polymarket', 0,
            'm-f', 't-f', 'BUY', 'LIMIT', 0.40, 2.5, 1.00,
            'accepted', '{}'::jsonb, now() - interval '79 minutes'
        )
        """,
        (intent_a, signal_a, intent_f, signal_f),
    )

    await db.execute(
        """
        insert into arb_order_event(
            intent_id, event_type, status_code, message, payload, created_at
        ) values (
            %s, 'submitted', 200, 'ok', '{}'::jsonb, now() - interval '88 minutes'
        ), (
            %s, 'submitted', 200, 'ok', '{}'::jsonb, now() - interval '78 minutes'
        )
        """,
        (intent_a, intent_f),
    )

    await db.execute(
        """
        insert into arb_trade_result(
            signal_id, mode, strategy_code, source_code, status, gross_pnl_usd, fees_usd,
            slippage_usd, net_pnl_usd, capital_used_usd, hold_minutes, opened_at, closed_at,
            metadata, created_at
        ) values (
            %s, 'paper_live', 'A', 'polymarket', 'filled', 0.06, 0.01, 0.00, 0.05,
            1.00, 10, now() - interval '90 minutes', now() - interval '80 minutes',
            %s::jsonb, now() - interval '80 minutes'
        ), (
            %s, 'paper_live', 'F', 'polymarket', 'filled', -0.01, 0.01, 0.00, -0.02,
            1.00, 20, now() - interval '80 minutes', now() - interval '60 minutes',
            %s::jsonb, now() - interval '60 minutes'
        )
        """,
        (
            signal_a,
            '{"run_tag":"unit-report"}',
            signal_f,
            '{"run_tag":"unit-report"}',
        ),
    )

    await db.execute(
        """
        insert into arb_risk_event(
            mode, strategy_code, source_code, event_type, severity, reason, payload, created_at
        ) values (
            'paper_live', 'G', 'polymarket', 'reject', 'warning', 'daily_stop_loss_triggered',
            %s::jsonb, now() - interval '55 minutes'
        )
        """,
        ('{"run_tag":"unit-report"}',),
    )

    result = await reporter.summary(
        since_hours=12,
        mode="paper_live",
        source_code="polymarket",
        run_tag=run_tag,
    )
    totals = result["totals"]

    assert int(totals["signals_found"]) == 3
    assert int(totals["signals_executed"]) == 2
    assert int(totals["trades"]) == 2
    assert totals["execution_rate"] == pytest.approx(2 / 3)
    assert totals["trade_conversion_rate"] == pytest.approx(1.0)
    assert totals["win_rate"] == pytest.approx(0.5)
    assert totals["net_pnl_usd"] == pytest.approx(0.03)

    by_strategy = {row["strategy_code"]: row for row in result["by_strategy"]}
    assert by_strategy["A"]["signals_found"] == 1
    assert by_strategy["A"]["signals_executed"] == 1
    assert by_strategy["A"]["trades"] == 1
    assert by_strategy["A"]["net_pnl_usd"] == pytest.approx(0.05)
    assert by_strategy["A"]["win_rate"] == pytest.approx(1.0)
    assert by_strategy["A"]["avg_detection_to_event_ms"] > 0

    assert by_strategy["F"]["signals_found"] == 1
    assert by_strategy["F"]["trades"] == 1
    assert by_strategy["F"]["net_pnl_usd"] == pytest.approx(-0.02)

    assert by_strategy["G"]["signals_found"] == 1
    assert by_strategy["G"]["signals_executed"] == 0
    assert by_strategy["G"]["trades"] == 0
    assert by_strategy["G"]["execution_rate"] == 0.0

    assert result["risk_top"]
    assert result["risk_top"][0]["reason"] == "daily_stop_loss_triggered"


@pytest.mark.asyncio
async def test_arb_report_filters_by_run_tag(db) -> None:
    reporter = ArbReporter(db)
    await db.execute(
        """
        insert into arb_trade_result(
            signal_id, mode, strategy_code, source_code, status, gross_pnl_usd, fees_usd,
            slippage_usd, net_pnl_usd, capital_used_usd, hold_minutes, opened_at, closed_at,
            metadata, created_at
        ) values
        (gen_random_uuid(), 'paper_live', 'A', 'polymarket', 'filled', 0.01, 0, 0, 0.01, 1, 1, now(), now(), '{"run_tag":"tag-a"}'::jsonb, now()),
        (gen_random_uuid(), 'paper_live', 'A', 'polymarket', 'filled', 0.02, 0, 0, 0.02, 1, 1, now(), now(), '{"run_tag":"tag-b"}'::jsonb, now())
        """
    )

    rows = await reporter.report(group_by="strategy", run_tag="tag-a")
    assert len(rows) == 1
    assert rows[0]["strategy_code"] == "A"
    assert rows[0]["trades"] == 1
    assert rows[0]["net_pnl_usd"] == pytest.approx(0.01)
