"""
Sliding-window rate quota manager for the Hyperliquid Info API.

HL enforces 1200 weight / min / IP on a rolling 60-second window.
QuotaManager tracks (timestamp, weight) pairs in a deque and blocks
callers until there is room in the current window.

Design notes
------------
- asyncio.Lock protects the check-and-append critical section so two
  concurrent coroutines cannot both see "room available" and both commit,
  which would silently overspend the budget.
- The lock is *released before sleeping*, so concurrent waiters all make
  progress once the window slides; no convoy effect.
- stats() is intentionally lock-free: it is a synchronous method (no
  await points) so asyncio's single-threaded scheduler guarantees it runs
  atomically relative to coroutines.
- time_func is injectable (default time.time) to enable deterministic
  unit tests without real wall-clock sleeps.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time_module
from collections import deque
from collections.abc import Callable

from hl_liq_hunter.config import HL_RATE_BUDGET

log = logging.getLogger(__name__)


class QuotaManager:
    """Rolling-window weight quota manager."""

    WINDOW_SECONDS: float = 60.0
    POLL_INTERVAL: float = 0.05  # 50 ms — resolution when blocked

    def __init__(
        self,
        max_per_min: int = HL_RATE_BUDGET,
        time_func: Callable[[], float] = _time_module.time,
    ) -> None:
        self._max = max_per_min
        self._window: deque[tuple[float, int]] = deque()
        self._lock = asyncio.Lock()
        self._time = time_func

    # ── private helpers (call only when lock held OR from sync context) ───────

    def _purge(self) -> None:
        """Drop records that have slid out of the 60-second window."""
        cutoff = self._time() - self.WINDOW_SECONDS
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()

    def _usage(self) -> int:
        """Sum of weights currently in the window (call after _purge)."""
        return sum(w for _, w in self._window)

    # ── public API ────────────────────────────────────────────────────────────

    async def acquire(self, weight: int) -> None:
        """
        Consume *weight* units from the rolling-window budget.

        Blocks (with POLL_INTERVAL sleep between retries) until the window
        has room.  Releases the lock before each sleep so peer coroutines
        can make progress concurrently.

        Parameters
        ----------
        weight:
            Positive integer cost of the operation (e.g. 2 for
            clearinghouseState).  Must be > 0.

        Raises
        ------
        ValueError
            If weight <= 0.
        """
        if weight <= 0:
            raise ValueError(f"weight must be > 0, got {weight}")

        while True:
            async with self._lock:
                self._purge()
                if self._usage() + weight <= self._max:
                    self._window.append((self._time(), weight))
                    return
                log.debug(
                    "quota full: usage=%d/%d weight=%d — retry in %.0fms",
                    self._usage(),
                    self._max,
                    weight,
                    self.POLL_INTERVAL * 1000,
                )
            # Lock released — sleep before next attempt so other waiters
            # and the event loop can run.
            await asyncio.sleep(self.POLL_INTERVAL)

    def stats(self) -> dict[str, float | int]:
        """
        Return a best-effort snapshot of window state.

        Safe to call without the lock because it is synchronous (no await
        points) and asyncio is single-threaded.

        Returns
        -------
        dict with keys:
            usage_per_min  – total weight consumed in the current window
            max_per_min    – configured budget ceiling
            utilization    – usage / max  (0.0 – 1.0)
            queue_depth    – number of records currently in the window
        """
        self._purge()
        usage = self._usage()
        return {
            "usage_per_min": usage,
            "max_per_min": self._max,
            "utilization": usage / self._max if self._max > 0 else 0.0,
            "queue_depth": len(self._window),
        }
