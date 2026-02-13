from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SettlementBundle:
    """
    用于 paper 的“到期结算模拟”：
    - 这不是在猜胜负，而是在“已锁定收益”的前提下做记账闭环
    - 为了让 lot 级别可追溯，我们会选一个“记账赢家”把 payout 记到某一条腿上
    """

    strategy_code: str
    token_ids: tuple[str, ...]
    shares: float
    gross_payout_usd: float
    winner_token_id: str


def is_locked_full_set_bundle(
    *,
    strategy_code: str,
    outcome_sides: dict[str, str],
    shares_by_token: dict[str, float],
    tolerance_pct: float = 0.02,
) -> bool:
    """
    判断一组 token 是否属于“锁定收益”的结算型组合：
    - B: 同一 market 的 YES+NO 且份额近似相等 => payout 恒为 shares
    - A: neg_risk 事件的 YES 全集（需要外部确保完整性），这里仅做“腿侧一致 & 份额一致”检查
    - C: 不满足（需要真实结算源 / 转换机制），默认不做结算模拟
    """
    s = (strategy_code or "").strip().upper()
    if s == "C":
        return False
    if not shares_by_token:
        return False
    if any(v <= 0 for v in shares_by_token.values()):
        return False

    # 份额一致性：避免不是“全套”的情况被误判成锁定收益。
    sizes = list(shares_by_token.values())
    mn, mx = min(sizes), max(sizes)
    if mn <= 0:
        return False
    if (mx - mn) / mn > float(tolerance_pct):
        return False

    sides = {str(outcome_sides.get(t, "")).upper() for t in shares_by_token.keys()}
    if s == "B":
        return sides == {"YES", "NO"} and len(shares_by_token) == 2
    if s == "A":
        return sides == {"YES"} and len(shares_by_token) >= 2
    return False


def build_settlement_bundle(
    *,
    strategy_code: str,
    token_ids: list[str],
    shares_by_token: dict[str, float],
) -> SettlementBundle | None:
    if not token_ids:
        return None
    shares = min(float(shares_by_token[t]) for t in token_ids if t in shares_by_token)
    if shares <= 0:
        return None
    winner = sorted(token_ids)[0]
    return SettlementBundle(
        strategy_code=(strategy_code or "").strip().upper(),
        token_ids=tuple(token_ids),
        shares=shares,
        gross_payout_usd=shares,  # 每份合约到期兑付 $1
        winner_token_id=winner,
    )

