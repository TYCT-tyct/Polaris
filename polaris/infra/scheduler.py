from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TaskSpec:
    name: str
    interval_sec: int
    job: Callable[[], Awaitable[None]]
    jitter_ratio: float = 0.1


class AsyncScheduler:
    def __init__(self, tasks: list[TaskSpec]) -> None:
        self.tasks = tasks
        self._stop_event = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []

    async def run(self) -> None:
        self._workers = [asyncio.create_task(self._run_task(task)) for task in self.tasks]
        try:
            await self._stop_event.wait()
        finally:
            await self._shutdown_workers()

    async def stop(self) -> None:
        self._stop_event.set()
        await self._shutdown_workers()

    async def _shutdown_workers(self) -> None:
        if not self._workers:
            return
        for worker in self._workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def _run_task(self, task: TaskSpec) -> None:
        while not self._stop_event.is_set():
            started = asyncio.get_running_loop().time()
            try:
                await task.job()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduler task failed", extra={"job_name": task.name})
            elapsed = asyncio.get_running_loop().time() - started
            base = max(task.interval_sec - elapsed, 0)
            jitter = task.interval_sec * task.jitter_ratio
            sleep_for = max(base + random.uniform(-jitter, jitter), 0.1)
            await asyncio.sleep(sleep_for)
