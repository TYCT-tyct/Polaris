from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from polaris.arb.contracts import ExecutionPlan, FillEvent, TokenSnapshot

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RustPaperResult:
    fills: list[FillEvent]
    events: list[tuple[str, str, int, str, str]]
    capital_used_usd: float


async def simulate_paper_with_rust(
    plan: ExecutionPlan,
    snapshots: dict[str, TokenSnapshot],
    *,
    binary: str,
    timeout_sec: int,
    slippage_bps: int,
) -> RustPaperResult | None:
    payload = {
        "slippage_bps": int(max(0, slippage_bps)),
        "intents": [
            {
                "intent_id": str(intent.intent_id),
                "token_id": intent.token_id,
                "market_id": intent.market_id,
                "side": intent.side.upper(),
                "order_type": intent.order_type.upper(),
                "limit_price": intent.limit_price,
                "shares": intent.shares,
                "notional_usd": intent.notional_usd,
            }
            for intent in plan.intents
        ],
        "snapshots": {
            token_id: {
                "market_id": snap.market_id,
                "best_bid": snap.best_bid,
                "best_ask": snap.best_ask,
                "min_order_size": snap.min_order_size,
                "tick_size": snap.tick_size,
                "bids": [{"price": lvl.price, "size": lvl.size} for lvl in snap.bids],
                "asks": [{"price": lvl.price, "size": lvl.size} for lvl in snap.asks],
            }
            for token_id, snap in snapshots.items()
        },
    }
    try:
        process = await asyncio.create_subprocess_exec(
            binary,
            "simulate-paper",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        logger.info("rust bridge binary not found", extra={"binary": binary})
        return None
    except Exception:
        logger.exception("rust bridge spawn failed", extra={"binary": binary})
        return None

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(input=json.dumps(payload, ensure_ascii=True).encode("utf-8")),
            timeout=max(1, timeout_sec),
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        logger.warning("rust bridge timed out", extra={"binary": binary, "timeout_sec": timeout_sec})
        return None

    if process.returncode != 0:
        logger.warning(
            "rust bridge failed",
            extra={
                "binary": binary,
                "returncode": process.returncode,
                "stderr": stderr.decode("utf-8", errors="replace").strip()[:400],
            },
        )
        return None

    try:
        data = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning("rust bridge returned invalid json", extra={"binary": binary})
        return None

    fills: list[FillEvent] = []
    for row in data.get("fills", []):
        fills.append(
            FillEvent(
                token_id=str(row["token_id"]),
                market_id=str(row["market_id"]),
                side=str(row["side"]).upper(),
                fill_price=float(row["fill_price"]),
                fill_size=float(row["fill_size"]),
                fill_notional_usd=float(row["fill_notional_usd"]),
                fee_usd=float(row.get("fee_usd", 0.0)),
            )
        )

    events: list[tuple[str, str, int, str, str]] = []
    for row in data.get("events", []):
        try:
            events.append(
                (
                    str(row["intent_id"]),
                    str(row["event_type"]),
                    int(row["status_code"]),
                    str(row["message"]),
                    json.dumps(row.get("payload", {}), ensure_ascii=True),
                )
            )
        except Exception:
            continue

    return RustPaperResult(
        fills=fills,
        events=events,
        capital_used_usd=float(data.get("capital_used_usd", 0.0)),
    )

