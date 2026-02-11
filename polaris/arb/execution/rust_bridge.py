from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from polaris.arb.contracts import ExecutionPlan, FillEvent, TokenSnapshot

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RustPaperResult:
    fills: list[FillEvent]
    events: list[tuple[str, str, int, str, str]]
    capital_used_usd: float


def _build_payload(
    plan: ExecutionPlan,
    snapshots: dict[str, TokenSnapshot],
    *,
    slippage_bps: int,
) -> dict[str, Any]:
    return {
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


def _parse_result(data: dict[str, Any]) -> RustPaperResult:
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


class RustBridgeClient:
    def __init__(self, *, binary: str, timeout_sec: int, mode: str = "daemon") -> None:
        self._binary = binary
        self._timeout_sec = max(1, timeout_sec)
        self._mode = mode.strip().lower()
        self._process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def simulate(
        self,
        plan: ExecutionPlan,
        snapshots: dict[str, TokenSnapshot],
        *,
        slippage_bps: int,
    ) -> RustPaperResult | None:
        payload = _build_payload(plan, snapshots, slippage_bps=slippage_bps)
        if self._mode == "subprocess":
            data = await _run_subprocess(binary=self._binary, timeout_sec=self._timeout_sec, payload=payload)
            return _parse_result(data) if data is not None else None
        data = await self._run_daemon(payload)
        return _parse_result(data) if data is not None else None

    async def close(self) -> None:
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            self._stderr_task = None
        process = self._process
        self._process = None
        if process is None:
            return
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

    async def _run_daemon(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        async with self._lock:
            process = await self._ensure_daemon()
            if process is None or process.stdin is None or process.stdout is None:
                return None
            request_id = uuid4().hex
            request = {"id": request_id, "payload": payload}
            try:
                process.stdin.write(json.dumps(request, ensure_ascii=True).encode("utf-8") + b"\n")
                await process.stdin.drain()
                line = await asyncio.wait_for(process.stdout.readline(), timeout=self._timeout_sec)
            except asyncio.TimeoutError:
                logger.warning("rust daemon timed out", extra={"binary": self._binary})
                await self._reset_process()
                return None
            except Exception:
                logger.exception("rust daemon io failed", extra={"binary": self._binary})
                await self._reset_process()
                return None

            if not line:
                logger.warning("rust daemon returned empty response", extra={"binary": self._binary})
                await self._reset_process()
                return None
            try:
                envelope = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                logger.warning("rust daemon returned invalid json", extra={"binary": self._binary})
                await self._reset_process()
                return None

            if str(envelope.get("id")) != request_id:
                logger.warning("rust daemon response id mismatch", extra={"binary": self._binary})
                return None
            if not bool(envelope.get("ok", False)):
                logger.warning(
                    "rust daemon rejected request",
                    extra={"binary": self._binary, "error": str(envelope.get("error", ""))[:240]},
                )
                return None
            result = envelope.get("result")
            if not isinstance(result, dict):
                logger.warning("rust daemon response missing result", extra={"binary": self._binary})
                return None
            return result

    async def _ensure_daemon(self) -> asyncio.subprocess.Process | None:
        process = self._process
        if process is not None and process.returncode is None:
            return process
        try:
            process = await asyncio.create_subprocess_exec(
                self._binary,
                "daemon",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.info("rust daemon binary not found", extra={"binary": self._binary})
            return None
        except Exception:
            logger.exception("rust daemon spawn failed", extra={"binary": self._binary})
            return None
        self._process = process
        self._stderr_task = asyncio.create_task(self._pump_stderr(process))
        return process

    async def _pump_stderr(self, process: asyncio.subprocess.Process) -> None:
        if process.stderr is None:
            return
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    return
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    logger.warning("rust daemon stderr", extra={"binary": self._binary, "line": text[:400]})
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("rust daemon stderr reader failed", extra={"binary": self._binary})

    async def _reset_process(self) -> None:
        process = self._process
        self._process = None
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            self._stderr_task = None
        if process is None:
            return
        if process.returncode is None:
            process.kill()
            await process.wait()


async def _run_subprocess(
    *,
    binary: str,
    timeout_sec: int,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
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
        return json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning("rust bridge returned invalid json", extra={"binary": binary})
        return None
