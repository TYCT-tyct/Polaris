
import unittest
from datetime import datetime, timedelta, UTC
from uuid import uuid4

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import TokenSnapshot, PriceLevel, RunMode
from polaris.arb.strategies.strategy_a import StrategyA
from polaris.arb.strategies.strategy_b import StrategyB
from polaris.arb.strategies.strategy_c import StrategyC
from polaris.arb.strategies.strategy_f import StrategyF
from polaris.arb.strategies.strategy_g import StrategyG
from polaris.config import PolarisSettings

def make_config():
    return ArbConfig(
        default_mode="paper_live",
        scan_interval_sec=1, optimize_interval_sec=60, optimize_replay_days=1, optimize_paper_hours=1,
        max_signals_per_cycle=10, universe_max_hours=48, universe_token_limit=100,
        clob_books_batch_size=10, clob_books_max_concurrency=1, execution_concurrency=1,
        live_preflight_max_age_ms=1000, live_preflight_force_refresh=False,
        execution_backend="python", run_tag="test",
        rust_bridge_enabled=False, rust_bridge_mode="off", rust_bridge_bin="", rust_bridge_timeout_sec=1,
        signal_dedupe_ttl_sec=10, scope_block_cooldown_sec=10, safe_arbitrage_only=False,
        strategy_health_gate_enabled=False, strategy_health_window_hours=1, strategy_health_min_trades=1,
        strategy_health_max_loss_usd=100, strategy_health_min_win_rate=0.0, strategy_health_min_avg_trade_pnl_usd=0.0,
        min_order_notional_usd=1.0, single_risk_usd=100.0, max_exposure_usd=1000.0, daily_stop_loss_usd=100.0,
        consecutive_fail_limit=10, partial_fill_timeout_sec=1, patch_fill_threshold=0.9, unwind_fill_threshold=0.1,
        slippage_bps=10, fee_bps=0, live_order_type="LIMIT",
        paper_initial_bankroll_usd=1000, paper_split_by_strategy=False, paper_enforce_bankroll=False,
        enable_strategy_a=True, enable_strategy_b=True, enable_strategy_c=True, enable_strategy_f=True, enable_strategy_g=True,
        c_live_enabled=True, strategy_priority=("A", "B", "C", "F", "G"),
        a_min_edge_pct=0.02, b_min_edge_pct=0.02, c_min_edge_pct=0.02, c_max_candidates_per_event=1,
        f_min_prob=0.9, f_max_hours_to_resolve=48, f_min_annualized_return=0.1, f_max_spread=0.05, f_max_signals_per_cycle=1,
        g_max_hours_to_resolve=6, g_min_confidence=0.8, g_min_expected_edge_pct=0.02,
        ai_enabled=False, ai_mode="off", ai_quorum=1, ai_provider_order=(), ai_single_model=""
    )

def make_token(
    market_id="m1",
    token_id="t1",
    event_id="e1",
    side="YES",
    price=0.5,
    size=1000,
    is_neg_risk=False,
    market_end=None
):
    asks = (PriceLevel(price=price, size=size),) if price > 0 else ()
    return TokenSnapshot(
        token_id=token_id, market_id=market_id, event_id=event_id,
        market_question="Q", market_end=market_end or (datetime.now(tz=UTC) + timedelta(hours=24)),
        outcome_label=side, outcome_side=side, outcome_index=0,
        min_order_size=1.0, tick_size=0.01,
        best_bid=price-0.01 if price>0.01 else None, best_ask=price if price>0 else None,
        is_neg_risk=is_neg_risk,
        asks=asks
    )

class TestStrategies(unittest.TestCase):
    def setUp(self):
        self.config = make_config()

    def test_strategy_a_negrisk_sum(self):
        # 3 outcomes, prices 0.3, 0.3, 0.3 => Sum 0.9 => Edge 10%
        # Must be neg_risk=True
        t1 = make_token(market_id="m1", token_id="t1", event_id="e_negrisk", side="YES", price=0.3, is_neg_risk=True)
        t2 = make_token(market_id="m2", token_id="t2", event_id="e_negrisk", side="YES", price=0.3, is_neg_risk=True)
        t3 = make_token(market_id="m3", token_id="t3", event_id="e_negrisk", side="YES", price=0.3, is_neg_risk=True)

        strat = StrategyA(self.config)
        signals = strat.scan(RunMode.PAPER_LIVE, "test", [t1, t2, t3])

        self.assertEqual(len(signals), 1)
        sig = signals[0]
        self.assertAlmostEqual(sig.edge_pct, 0.10)
        print(f"\n[A] Detected: Sum 0.9, Edge 10% -> PASS")

    def test_strategy_b_binary(self):
        # YES=0.4, NO=0.4 => Sum 0.8 => Edge 20%
        t_yes = make_token(market_id="m_binary", token_id="t_yes", side="YES", price=0.4, is_neg_risk=False)
        t_no = make_token(market_id="m_binary", token_id="t_no", side="NO", price=0.4, is_neg_risk=False)

        strat = StrategyB(self.config)
        signals = strat.scan(RunMode.PAPER_LIVE, "test", [t_yes, t_no])

        self.assertEqual(len(signals), 1)
        sig = signals[0]
        self.assertAlmostEqual(sig.edge_pct, 0.20)
        print(f"\n[B] Detected: Binary Sum 0.8, Edge 20% -> PASS")

    def test_strategy_c_conversion(self):
        # NO=0.1. Complement YES (sum of others) = 0.5.
        # Buying NO @ 0.1 is like buying 'All Others' @ 0.1.
        # But separate YESs cost 0.5.
        # So we buy NO, convert to 'All Others YES', and sell?
        # Check code: edge_pct = complement_cost - no_price.
        # Complement YES sum (Outcome 2+3) = 0.4 + 0.4 = 0.8.
        # NO price = 0.1.
        # Edge = 0.8 - 0.1 = 0.7.

        t1_no = make_token(market_id="m1", token_id="t1_no", event_id="e_c", side="NO", price=0.1, is_neg_risk=True)
        t1_yes = make_token(market_id="m1", token_id="t1_yes", event_id="e_c", side="YES", price=0.9, is_neg_risk=True)

        t2_yes = make_token(market_id="m2", token_id="t2_yes", event_id="e_c", side="YES", price=0.4, is_neg_risk=True)
        t2_no = make_token(market_id="m2", token_id="t2_no", event_id="e_c", side="NO", price=0.6, is_neg_risk=True)

        t3_yes = make_token(market_id="m3", token_id="t3_yes", event_id="e_c", side="YES", price=0.4, is_neg_risk=True)
        t3_no = make_token(market_id="m3", token_id="t3_no", event_id="e_c", side="NO", price=0.6, is_neg_risk=True)

        strat = StrategyC(self.config)
        signals = strat.scan(RunMode.PAPER_LIVE, "test", [t1_no, t1_yes, t2_yes, t2_no, t3_yes, t3_no])

        self.assertEqual(len(signals), 1)
        sig = signals[0]
        self.assertAlmostEqual(sig.edge_pct, 0.7)
        self.assertEqual(sig.token_ids[0], "t1_no")
        print(f"\n[C] Detected: NO(0.1) vs Complement(0.8), Edge 0.7 -> PASS")

    def test_strategy_f_bond(self):
        # YES price 0.96. Expires in 2 days (48h).
        # Annualized: (0.04/0.96) * 0.98 * (365/2 days)
        # = 0.0416 * 0.98 * 182.5 = 7.4. > 10% min.
        end = datetime.now(tz=UTC) + timedelta(days=2)
        t = make_token(market_id="m_bond", side="YES", price=0.96, market_end=end)

        strat = StrategyF(self.config)
        signals = strat.scan(RunMode.PAPER_LIVE, "test", [t])

        self.assertEqual(len(signals), 1)
        print(f"\n[F] Detected: Price 0.96, 2 Days left -> PASS")

    def test_strategy_g_tail(self):
        # 4 hours left.
        # Top YES = 0.85. 2nd YES = 0.05.
        # Dominance = 0.8.
        # Time factor = (6 - 4) / 6 = 0.33.
        # Confidence = 0.85 + 0.8 * (0.15 + 0.35 * 0.33) = 0.85 + 0.8 * 0.2655 = 0.85 + 0.21 = 1.06 -> capped 0.999.
        # Exp Edge = 0.999 - 0.85 = 0.149.
        # > 0.02 min edge.
        # < 0.97 price.
        end = datetime.now(tz=UTC) + timedelta(hours=4)
        t1 = make_token(market_id="m_tail", token_id="t1", event_id="e_tail", side="YES", price=0.85, market_end=end)
        t2 = make_token(market_id="m_tail", token_id="t2", event_id="e_tail", side="YES", price=0.05, market_end=end)

        strat = StrategyG(self.config)
        signals = strat.scan(RunMode.PAPER_LIVE, "test", [t1, t2])

        self.assertEqual(len(signals), 1)
        print(f"\n[G] Detected: 4h left, 0.85 dominant -> PASS")

if __name__ == '__main__':
    unittest.main()
