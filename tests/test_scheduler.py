import asyncio

import pytest

from polaris.infra.scheduler import AsyncScheduler, TaskSpec


@pytest.mark.asyncio
async def test_scheduler_cancelled_run_cleans_workers() -> None:
    job_started = asyncio.Event()
    job_cancelled = asyncio.Event()

    async def job() -> None:
        job_started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            job_cancelled.set()
            raise

    scheduler = AsyncScheduler(
        [
            TaskSpec(
                name="long_job",
                interval_sec=60,
                job=job,
                jitter_ratio=0,
            )
        ]
    )
    run_task = asyncio.create_task(scheduler.run())

    await asyncio.wait_for(job_started.wait(), timeout=1)
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert job_cancelled.is_set()
    assert scheduler._workers == []


@pytest.mark.asyncio
async def test_scheduler_stop_returns_and_cleans_workers() -> None:
    tick = asyncio.Event()

    async def job() -> None:
        tick.set()
        await asyncio.sleep(10)

    scheduler = AsyncScheduler(
        [TaskSpec(name="job", interval_sec=60, job=job, jitter_ratio=0)]
    )
    run_task = asyncio.create_task(scheduler.run())

    await asyncio.wait_for(tick.wait(), timeout=1)
    await scheduler.stop()
    await asyncio.wait_for(run_task, timeout=1)

    assert scheduler._workers == []
