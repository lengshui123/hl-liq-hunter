"""
Phase 2 — Module 3: TierScanner.

Per-tier address scanner.  Dequeues due/dirty addresses from AddressRegistry,
queries HLClient with bounded concurrency, parses positions, updates registry,
and streams rows to SnapshotWriter.

One TierScanner instance per tier.  Four instances (DIRTY, WHALE, MIDTIER,
LONGTAIL) share a single HLClient and QuotaManager, so the 900 weight/min
budget is enforced globally across all concurrent scanners.
"""

from __future__ import annotations

import asyncio
import logging
import time

from hl_liq_hunter.collector.parser import parse_positions
from hl_liq_hunter.collector.writer import SnapshotWriter
from hl_liq_hunter.config import (
    PHASE2_INTER_BATCH_SLEEP_SEC,
    PHASE2_SCANNER_CONCURRENCY,
    PHASE2_SKIP_NULL_LIQ_PX,
)
from hl_liq_hunter.config import Tier
from hl_liq_hunter.core.address_registry import AddressRegistry
from hl_liq_hunter.core.hl_client import HLClient

logger = logging.getLogger(__name__)

# Sleep after an unexpected exception in run() before retrying.
_RUN_ERROR_BACKOFF_SEC: float = 5.0


class TierScanner:
    """
    Async scanner for a single address tier.

    Lifecycle:
        1. Instantiate with shared client, registry, writer.
        2. asyncio.create_task(scanner.run()) — runs until cancelled.
        3. Call scanner.stats() at any time for observability.
    """

    def __init__(
        self,
        tier: Tier,
        client: HLClient,
        registry: AddressRegistry,
        writer: SnapshotWriter,
        *,
        batch_size: int = 50,
        concurrency: int = PHASE2_SCANNER_CONCURRENCY,
        inter_batch_sleep_sec: float = PHASE2_INTER_BATCH_SLEEP_SEC,
        idle_sleep_sec: float = 5.0,
    ) -> None:
        self._tier = tier
        self._client = client
        self._registry = registry
        self._writer = writer
        self._batch_size = batch_size
        self._concurrency = concurrency
        self._inter_batch_sleep_sec = inter_batch_sleep_sec
        self._idle_sleep_sec = idle_sleep_sec

        # Stats — updated without a lock (single event loop thread, no data race).
        self._scans_completed: int = 0
        self._scans_failed: int = 0
        self._last_batch_size: int = 0
        self._last_batch_at: float = 0.0
        self._null_liq_px_total: int = 0

    # ── public API ────────────────────────────────────────────────────────────

    async def scan_one(self, address: str) -> None:
        """
        Query one address → parse → update registry → write rows.

        Never raises.  All exceptions are caught, logged, and counted in
        scans_failed so a single bad address cannot break the whole batch.

        If clearinghouse_state returns None (rate-limited / network error),
        last_scanned is NOT updated so the address remains due on the next pass.
        """
        try:
            now = time.time()
            now_ms = int(now * 1000)

            state = await self._client.clearinghouse_state(address)
            if state is None:
                logger.debug(
                    "[%s] clearinghouse_state returned None for %s — skipping update",
                    self._tier.name, address,
                )
                return

            result = parse_positions(
                address,
                state,
                now_ms,
                skip_null_liq_px=PHASE2_SKIP_NULL_LIQ_PX,
            )

            if result.rows:
                await self._writer.add(result.rows)

            await self._registry.update_after_scan(
                address, result.total_usd, result.has_position, now
            )

            self._scans_completed += 1
            self._null_liq_px_total += result.null_liq_count

        except Exception:
            self._scans_failed += 1
            logger.error(
                "[%s] scan_one failed for address=%s",
                self._tier.name, address,
                exc_info=True,
            )

    async def scan_batch(self, addresses: list[str]) -> None:
        """
        Concurrently scan up to `concurrency` addresses, then sleep
        inter_batch_sleep_sec to rate-smooth between batches.

        return_exceptions=True ensures one failed coroutine cannot cancel
        the rest.  Because scan_one never raises, exceptions here signal an
        unexpected bug (e.g. semaphore deadlock) — they are counted separately.
        """
        sem = asyncio.Semaphore(self._concurrency)

        async def _scan_limited(addr: str) -> None:
            async with sem:
                await self.scan_one(addr)

        results = await asyncio.gather(
            *(_scan_limited(a) for a in addresses),
            return_exceptions=True,
        )

        for r in results:
            if isinstance(r, Exception):
                # scan_one should never raise, so this is truly unexpected.
                self._scans_failed += 1
                logger.error(
                    "[%s] unexpected exception escaped scan_one: %s",
                    self._tier.name, r,
                )

        self._last_batch_size = len(addresses)
        self._last_batch_at = time.time()

        await asyncio.sleep(self._inter_batch_sleep_sec)

    async def run(self) -> None:
        """
        Main scanner loop.  Runs until cancelled.

        Flow per iteration:
            1. Fetch a batch of due/dirty addresses from registry.
            2. If empty → idle sleep, loop.
            3. If non-empty → scan_batch (concurrent queries + inter-batch sleep).

        Unexpected exceptions are logged and followed by a backoff sleep so a
        transient registry/network hiccup does not spin the loop at CPU speed.
        CancelledError propagates immediately after logging.
        """
        logger.info("[%s] scanner started", self._tier.name)
        while True:
            try:
                if self._tier == Tier.DIRTY:
                    addresses = await self._registry.get_dirty_addresses(self._batch_size)
                else:
                    addresses = await self._registry.get_due_addresses(
                        self._tier, self._batch_size
                    )

                if not addresses:
                    await asyncio.sleep(self._idle_sleep_sec)
                    continue

                await self.scan_batch(addresses)

            except asyncio.CancelledError:
                logger.info("[%s] scanner cancelled, exiting", self._tier.name)
                raise
            except Exception as exc:
                logger.error(
                    "[%s] unexpected error in run loop: %s",
                    self._tier.name, exc,
                    exc_info=True,
                )
                await asyncio.sleep(_RUN_ERROR_BACKOFF_SEC)

    def stats(self) -> dict[str, object]:
        """Lock-free snapshot of scanner state."""
        return {
            "scans_completed":   self._scans_completed,
            "scans_failed":      self._scans_failed,
            "last_batch_size":   self._last_batch_size,
            "last_batch_at":     self._last_batch_at,
            "null_liq_px_total": self._null_liq_px_total,
        }
