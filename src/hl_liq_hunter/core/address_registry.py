"""
Tiered address registry for Hyperliquid trader addresses.

Addresses are classified into WHALE / MIDTIER / LONGTAIL based on open
position size and re-scanned at tier-appropriate intervals.  Addresses
recently seen in the trade stream are flagged "dirty" for priority
re-scanning; their dirtiness is expressed via a dirty_until timestamp —
AddressRecord.tier is NEVER set to Tier.DIRTY.

Design notes
------------
- asyncio.Lock serialises all mutations; reads inside the lock capture
  plain-value tuples so the lock can be released before sorting.
- stats() is lock-free and synchronous (safe in asyncio's single-threaded
  scheduler — no coroutine can preempt it).
- save_to_parquet holds the lock only for the in-memory copy; the actual
  disk write runs outside the lock (and in a thread pool via
  asyncio.to_thread) to avoid blocking the event loop.
- time_func is injectable (default time.time) for deterministic tests.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time_module
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from typing import Any

import pandas as pd

from hl_liq_hunter.config import (
    DIRTY_MIN_RESCAN_SEC,
    DIRTY_SET_TTL_SEC,
    TIER_SCAN_INTERVAL_SEC,
    TIER_THRESHOLDS_USD,
    Tier,
)

log = logging.getLogger(__name__)


@dataclass
class AddressRecord:
    """Per-address state.  tier is always WHALE/MIDTIER/LONGTAIL — never DIRTY."""

    address: str
    tier: Tier = Tier.LONGTAIL
    last_scanned: float = 0.0
    total_usd: float = 0.0
    has_position: bool = False
    dirty_until: float = 0.0
    consecutive_empty: int = 0


class AddressRegistry:
    """Async-safe registry of HL trader addresses with tier management."""

    def __init__(
        self,
        time_func: Callable[[], float] = _time_module.time,
    ) -> None:
        self._records: dict[str, AddressRecord] = {}
        self._lock = asyncio.Lock()
        self._time = time_func

    # ── Mutation methods ──────────────────────────────────────────────────────

    async def add_address(self, address: str) -> None:
        """Add *address* with default tier LONGTAIL.  No-op if already present."""
        async with self._lock:
            if address not in self._records:
                self._records[address] = AddressRecord(address=address)

    async def mark_dirty(self, address: str) -> None:
        """
        Flag *address* as dirty for DIRTY_SET_TTL_SEC seconds.

        Creates the record with defaults if the address is not yet known.
        """
        async with self._lock:
            if address not in self._records:
                self._records[address] = AddressRecord(address=address)
            self._records[address].dirty_until = self._time() + DIRTY_SET_TTL_SEC

    async def update_after_scan(
        self,
        address: str,
        total_usd: float,
        has_position: bool,
        scanned_at: float,
    ) -> None:
        """
        Update record after a clearinghouseState scan and recalculate tier.

        Tier logic
        ----------
        has_position=True:
            total_usd >= $1M  → WHALE
            total_usd >= $50k → MIDTIER
            else              → LONGTAIL
            consecutive_empty reset to 0.
        has_position=False:
            tier → LONGTAIL
            consecutive_empty incremented.

        dirty_until is intentionally left unchanged (dirty state is
        managed independently by mark_dirty).
        """
        async with self._lock:
            if address not in self._records:
                self._records[address] = AddressRecord(address=address)
            rec = self._records[address]
            rec.total_usd = total_usd
            rec.has_position = has_position
            rec.last_scanned = scanned_at

            if has_position:
                if total_usd >= TIER_THRESHOLDS_USD[Tier.WHALE]:
                    rec.tier = Tier.WHALE
                elif total_usd >= TIER_THRESHOLDS_USD[Tier.MIDTIER]:
                    rec.tier = Tier.MIDTIER
                else:
                    rec.tier = Tier.LONGTAIL
                rec.consecutive_empty = 0
            else:
                rec.tier = Tier.LONGTAIL
                rec.consecutive_empty += 1

    # ── Query methods ─────────────────────────────────────────────────────────

    async def get_due_addresses(
        self,
        tier: Tier,
        limit: int = 500,
    ) -> list[str]:
        """
        Return up to *limit* non-dirty addresses in *tier* that are due
        for a re-scan, ordered by last_scanned ascending (oldest first).

        Due = tier matches AND (now - last_scanned) >= interval AND NOT dirty.
        """
        now = self._time()
        interval = TIER_SCAN_INTERVAL_SEC[tier]
        async with self._lock:
            # Capture (last_scanned, address) tuples while holding the lock
            # so the values are stable during sorting.
            candidates: list[tuple[float, str]] = [
                (rec.last_scanned, rec.address)
                for rec in self._records.values()
                if rec.tier == tier
                and rec.dirty_until <= now
                and (now - rec.last_scanned) >= interval
            ]
        candidates.sort()
        return [addr for _, addr in candidates[:limit]]

    async def get_dirty_addresses(
        self,
        limit: int = 200,
    ) -> list[str]:
        """
        Return up to *limit* dirty addresses that are due for a re-scan,
        ordered by last_scanned ascending.

        Due = dirty_until > now AND (now - last_scanned) >= DIRTY_MIN_RESCAN_SEC.
        """
        now = self._time()
        async with self._lock:
            candidates: list[tuple[float, str]] = [
                (rec.last_scanned, rec.address)
                for rec in self._records.values()
                if rec.dirty_until > now
                and (now - rec.last_scanned) >= DIRTY_MIN_RESCAN_SEC
            ]
        candidates.sort()
        return [addr for _, addr in candidates[:limit]]

    # ── Persistence ───────────────────────────────────────────────────────────

    async def save_to_parquet(self, path: Path) -> None:
        """
        Serialise all records to *path* as parquet.

        The lock is held only for the in-memory snapshot; the disk write
        runs in a thread pool to avoid blocking the event loop.
        """
        async with self._lock:
            rows = [
                {
                    "address":          rec.address,
                    "tier":             int(rec.tier),
                    "last_scanned":     rec.last_scanned,
                    "total_usd":        rec.total_usd,
                    "has_position":     rec.has_position,
                    "dirty_until":      rec.dirty_until,
                    "consecutive_empty": rec.consecutive_empty,
                }
                for rec in self._records.values()
            ]
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(rows)
        await asyncio.to_thread(df.to_parquet, path, index=False)
        log.info("Saved %d records to %s", len(rows), path)

    @classmethod
    async def load_from_parquet(
        cls,
        path: Path,
        time_func: Callable[[], float] = _time_module.time,
    ) -> "AddressRegistry":
        """
        Load an AddressRegistry from *path*.

        Returns an empty registry without raising if the file does not exist.
        """
        registry = cls(time_func=time_func)
        if not path.exists():
            log.info("Registry file %s not found — starting empty", path)
            return registry

        df: pd.DataFrame = await asyncio.to_thread(pd.read_parquet, path)
        for row in df.itertuples(index=False):
            r: Any = row  # pandas-stubs gives itertuples() an overly-wide union type
            rec = AddressRecord(
                address=str(r.address),
                tier=Tier(int(r.tier)),
                last_scanned=float(r.last_scanned),
                total_usd=float(r.total_usd),
                has_position=bool(r.has_position),
                dirty_until=float(r.dirty_until),
                consecutive_empty=int(r.consecutive_empty),
            )
            registry._records[rec.address] = rec
        log.info("Loaded %d records from %s", len(registry._records), path)
        return registry

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, object]:
        """
        Return a best-effort snapshot of registry state.

        Lock-free and synchronous; safe in asyncio's single-threaded scheduler.

        by_tier counts records by their stored tier (WHALE / MIDTIER / LONGTAIL).
        dirty_count is kept as a separate key because dirty is a transient
        *state*, not a tier value stored in the record.
        """
        now = self._time()
        by_tier: dict[str, int] = {
            Tier.WHALE.name:    0,
            Tier.MIDTIER.name:  0,
            Tier.LONGTAIL.name: 0,
        }
        active_holders = 0
        dirty_count = 0

        for rec in self._records.values():
            name = rec.tier.name
            if name in by_tier:
                by_tier[name] += 1
            if rec.has_position:
                active_holders += 1
            if rec.dirty_until > now:
                dirty_count += 1

        return {
            "total":          len(self._records),
            "by_tier":        by_tier,
            "active_holders": active_holders,
            "dirty_count":    dirty_count,
        }
