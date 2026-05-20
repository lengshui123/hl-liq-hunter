"""
Tests for QuotaManager.

Time is injected via FakeClock so no test ever waits a real 60 seconds.
The real asyncio.sleep(POLL_INTERVAL) calls are short (~50 ms each) and
only happen in the "at_limit_blocks" and "concurrent" tests that need
the event loop to actually yield between coroutines.
"""

from __future__ import annotations

import asyncio
import time as real_time

import pytest

from hl_liq_hunter.core.quota_manager import QuotaManager


# ── Shared test helper ────────────────────────────────────────────────────────

class FakeClock:
    """Mutable monotonic clock injectable into QuotaManager."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


# ── Test cases ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_under_limit_no_sleep() -> None:
    """10 × weight=2 = 20, well under budget=1000 — zero blocking time."""
    clock = FakeClock()
    qm = QuotaManager(max_per_min=1000, time_func=clock)

    wall_start = real_time.monotonic()
    for _ in range(10):
        await qm.acquire(2)
    elapsed = real_time.monotonic() - wall_start

    # No sleep should occur; 100 ms is a generous upper bound.
    assert elapsed < 0.1, f"Unexpected sleep: {elapsed:.3f}s"
    assert qm.stats()["usage_per_min"] == 20


@pytest.mark.asyncio
async def test_at_limit_blocks() -> None:
    """After the budget is exhausted, acquire blocks until the window slides."""
    clock = FakeClock(start=0.0)
    qm = QuotaManager(max_per_min=10, time_func=clock)

    await qm.acquire(10)  # exhaust the budget at t=0

    acquired = asyncio.Event()

    async def blocked_caller() -> None:
        await qm.acquire(1)
        acquired.set()

    task = asyncio.create_task(blocked_caller())
    # Yield once so the task can run its first poll iteration and
    # reach asyncio.sleep(POLL_INTERVAL).
    await asyncio.sleep(0)
    assert not acquired.is_set(), "acquire should still be blocked"

    # Slide the window: the record at t=0 is now >60 s old.
    clock.advance(61.0)

    # The task's next poll (≤50 ms real time) will purge the old record
    # and succeed.  Give it up to 1 s wall-clock.
    await asyncio.wait_for(acquired.wait(), timeout=1.0)
    assert acquired.is_set()
    task.cancel()


@pytest.mark.asyncio
async def test_sliding_window_purge() -> None:
    """Records older than 60 s are purged; recent records are kept."""
    clock = FakeClock(start=100.0)
    qm = QuotaManager(max_per_min=1000, time_func=clock)

    # Inject records directly at known timestamps.
    # cutoff = 100.0 - 60.0 = 40.0
    qm._window.append((30.0, 50))   # 70 s old  → purged  (30 < 40)
    qm._window.append((39.9, 20))   # 60.1 s old → purged  (39.9 < 40)
    qm._window.append((40.1, 15))   # 59.9 s old → kept    (40.1 ≥ 40)
    qm._window.append((99.0,  5))   # 1 s old    → kept

    s = qm.stats()  # triggers _purge internally
    assert s["usage_per_min"] == 20  # 15 + 5
    assert s["queue_depth"] == 2


@pytest.mark.asyncio
async def test_concurrent_acquire() -> None:
    """100 coroutines each acquire weight=2; total=200 < budget=1000."""
    clock = FakeClock()
    qm = QuotaManager(max_per_min=1000, time_func=clock)

    wall_start = real_time.monotonic()
    await asyncio.gather(*[qm.acquire(2) for _ in range(100)])
    elapsed = real_time.monotonic() - wall_start

    assert elapsed < 2.0, f"Concurrent acquire too slow: {elapsed:.3f}s"
    assert qm.stats()["usage_per_min"] == 200
    assert qm.stats()["queue_depth"] == 100


@pytest.mark.asyncio
async def test_zero_weight_rejected() -> None:
    """acquire(0) must raise ValueError immediately."""
    qm = QuotaManager()
    with pytest.raises(ValueError, match="weight must be > 0"):
        await qm.acquire(0)


@pytest.mark.asyncio
async def test_negative_weight_rejected() -> None:
    """acquire(-5) must raise ValueError immediately."""
    qm = QuotaManager()
    with pytest.raises(ValueError, match="weight must be > 0"):
        await qm.acquire(-5)


@pytest.mark.asyncio
async def test_stats_accuracy() -> None:
    """stats() fields are numerically correct after several acquires."""
    clock = FakeClock()
    qm = QuotaManager(max_per_min=500, time_func=clock)

    await qm.acquire(100)
    await qm.acquire(50)
    await qm.acquire(75)

    s = qm.stats()
    assert s["usage_per_min"] == 225
    assert s["max_per_min"] == 500
    assert abs(s["utilization"] - 0.45) < 1e-9
    assert s["queue_depth"] == 3


@pytest.mark.asyncio
async def test_window_resets_after_60s() -> None:
    """Usage drops to 0 once all records have slid out of the 60 s window."""
    clock = FakeClock(start=0.0)
    qm = QuotaManager(max_per_min=100, time_func=clock)

    await qm.acquire(80)
    assert qm.stats()["usage_per_min"] == 80

    clock.advance(61.0)
    assert qm.stats()["usage_per_min"] == 0
    assert qm.stats()["queue_depth"] == 0


@pytest.mark.asyncio
async def test_multiple_waiters_all_unblock() -> None:
    """
    Three coroutines queue up behind an exhausted budget; after the window
    slides they all complete without racing each other over the limit.
    """
    clock = FakeClock(start=0.0)
    qm = QuotaManager(max_per_min=10, time_func=clock)

    await qm.acquire(10)  # full at t=0

    results: list[int] = []

    async def waiter(n: int) -> None:
        await qm.acquire(3)
        results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0)  # let all tasks start their first poll

    assert results == [], "no waiter should have acquired yet"

    clock.advance(61.0)  # slide the window

    # All three need 3 weight each = 9 ≤ 10 — they can all fit.
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1.0)
    assert sorted(results) == [0, 1, 2]
