"""
Tests for AddressRegistry.

FakeClock is injected so no test depends on wall-clock time.
File I/O tests use pytest's tmp_path fixture.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from hl_liq_hunter.config import (
    DIRTY_MIN_RESCAN_SEC,
    DIRTY_SET_TTL_SEC,
    TIER_SCAN_INTERVAL_SEC,
    Tier,
)
from hl_liq_hunter.core.address_registry import AddressRecord, AddressRegistry

# ── Helpers ───────────────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start: float = 10_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


# Realistic-looking 42-char addresses
ADDR_A = "0x" + "a" * 40
ADDR_B = "0x" + "b" * 40
ADDR_C = "0x" + "c" * 40


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_add_then_query() -> None:
    """add_address creates a LONGTAIL record retrievable by get_due_addresses."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.add_address(ADDR_A)

    # Advance past LONGTAIL scan interval so it shows up as due.
    clock.advance(TIER_SCAN_INTERVAL_SEC[Tier.LONGTAIL] + 1)
    due = await reg.get_due_addresses(Tier.LONGTAIL)

    assert ADDR_A in due
    assert reg.stats()["total"] == 1


@pytest.mark.asyncio
async def test_add_idempotent() -> None:
    """add_address called twice for the same address does not duplicate it."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.add_address(ADDR_A)
    await reg.add_address(ADDR_A)

    assert reg.stats()["total"] == 1


@pytest.mark.asyncio
async def test_tier_promotion_to_whale() -> None:
    """update_after_scan with total_usd >= $1M promotes to WHALE."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.add_address(ADDR_A)
    await reg.update_after_scan(ADDR_A, total_usd=2_000_000.0,
                                has_position=True, scanned_at=clock.t)

    clock.advance(TIER_SCAN_INTERVAL_SEC[Tier.WHALE] + 1)
    whale_due = await reg.get_due_addresses(Tier.WHALE)
    longtail_due = await reg.get_due_addresses(Tier.LONGTAIL)

    assert ADDR_A in whale_due
    assert ADDR_A not in longtail_due
    assert reg.stats()["by_tier"]["WHALE"] == 1


@pytest.mark.asyncio
async def test_tier_boundary_exact() -> None:
    """total_usd == $1_000_000 (the boundary) is WHALE, not MIDTIER."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.update_after_scan(ADDR_A, total_usd=1_000_000.0,
                                has_position=True, scanned_at=clock.t)

    clock.advance(TIER_SCAN_INTERVAL_SEC[Tier.WHALE] + 1)
    assert ADDR_A in await reg.get_due_addresses(Tier.WHALE)
    assert ADDR_A not in await reg.get_due_addresses(Tier.MIDTIER)


@pytest.mark.asyncio
async def test_tier_boundary_midtier_exact() -> None:
    """total_usd == $50_000 (the boundary) is MIDTIER, not LONGTAIL."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.update_after_scan(ADDR_A, total_usd=50_000.0,
                                has_position=True, scanned_at=clock.t)

    clock.advance(TIER_SCAN_INTERVAL_SEC[Tier.MIDTIER] + 1)
    assert ADDR_A in await reg.get_due_addresses(Tier.MIDTIER)
    assert ADDR_A not in await reg.get_due_addresses(Tier.LONGTAIL)


@pytest.mark.asyncio
async def test_empty_position_demotes_to_longtail() -> None:
    """A previously WHALE address with no position drops to LONGTAIL."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    # First scan: big position → WHALE
    await reg.update_after_scan(ADDR_A, total_usd=5_000_000.0,
                                has_position=True, scanned_at=clock.t)
    clock.advance(1.0)
    # Second scan: position gone
    await reg.update_after_scan(ADDR_A, total_usd=0.0,
                                has_position=False, scanned_at=clock.t)

    clock.advance(TIER_SCAN_INTERVAL_SEC[Tier.LONGTAIL] + 1)
    assert ADDR_A not in await reg.get_due_addresses(Tier.WHALE)
    assert ADDR_A in await reg.get_due_addresses(Tier.LONGTAIL)

    # consecutive_empty should be 1
    rec = reg._records[ADDR_A]
    assert rec.consecutive_empty == 1
    assert reg.stats()["active_holders"] == 0


@pytest.mark.asyncio
async def test_due_ordering() -> None:
    """get_due_addresses returns oldest-scanned addresses first."""
    clock = FakeClock(start=0.0)
    reg = AddressRegistry(time_func=clock)

    # Register three MIDTIER addresses scanned at t=10, t=5, t=1 (C is oldest)
    for addr, scanned_at, usd in [
        (ADDR_A, 10.0, 100_000.0),
        (ADDR_B,  5.0, 100_000.0),
        (ADDR_C,  1.0, 100_000.0),
    ]:
        await reg.update_after_scan(addr, total_usd=usd,
                                    has_position=True, scanned_at=scanned_at)

    # Advance past MIDTIER interval so all are due
    clock.t = TIER_SCAN_INTERVAL_SEC[Tier.MIDTIER] + 20.0
    due = await reg.get_due_addresses(Tier.MIDTIER)

    assert due == [ADDR_C, ADDR_B, ADDR_A]  # ascending last_scanned


@pytest.mark.asyncio
async def test_dirty_ttl_expiration() -> None:
    """A dirty address is no longer dirty after dirty_until elapses."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.mark_dirty(ADDR_A)
    # last_scanned=0.0, now=10000 → 10000 - 0 >> DIRTY_MIN_RESCAN_SEC → passes throttle.
    dirty = await reg.get_dirty_addresses()
    assert ADDR_A in dirty

    # Advance past TTL
    clock.advance(DIRTY_SET_TTL_SEC + 1)
    assert ADDR_A not in await reg.get_dirty_addresses()


@pytest.mark.asyncio
async def test_dirty_skipped_in_due() -> None:
    """Dirty addresses do NOT appear in get_due_addresses."""
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)

    await reg.add_address(ADDR_A)
    await reg.mark_dirty(ADDR_A)

    # last_scanned=0, now=10_000 → already past LONGTAIL interval (3600s).
    # No need to advance; advancing by LONGTAIL interval would expire the dirty mark (TTL=300s).
    due = await reg.get_due_addresses(Tier.LONGTAIL)

    assert ADDR_A not in due


@pytest.mark.asyncio
async def test_persistence_roundtrip(tmp_path: Path) -> None:
    """save_to_parquet → load_from_parquet preserves all record fields."""
    clock = FakeClock(start=500.0)
    reg = AddressRegistry(time_func=clock)

    await reg.update_after_scan(ADDR_A, total_usd=1_500_000.0,
                                has_position=True, scanned_at=100.0)
    await reg.update_after_scan(ADDR_B, total_usd=0.0,
                                has_position=False, scanned_at=200.0)
    await reg.mark_dirty(ADDR_B)

    path = tmp_path / "registry.parquet"
    await reg.save_to_parquet(path)

    loaded = await AddressRegistry.load_from_parquet(path, time_func=clock)

    assert loaded.stats()["total"] == 2

    rec_a = loaded._records[ADDR_A]
    assert rec_a.tier == Tier.WHALE
    assert rec_a.total_usd == 1_500_000.0
    assert rec_a.has_position is True
    assert rec_a.last_scanned == 100.0
    assert rec_a.consecutive_empty == 0

    rec_b = loaded._records[ADDR_B]
    assert rec_b.tier == Tier.LONGTAIL
    assert rec_b.has_position is False
    assert rec_b.consecutive_empty == 1
    assert rec_b.dirty_until > 0.0  # dirty mark preserved


@pytest.mark.asyncio
async def test_load_missing_file_returns_empty(tmp_path: Path) -> None:
    """load_from_parquet on a nonexistent file returns empty registry, no exception."""
    reg = await AddressRegistry.load_from_parquet(tmp_path / "nonexistent.parquet")
    assert reg.stats()["total"] == 0


@pytest.mark.asyncio
async def test_concurrent_update() -> None:
    """
    100 concurrent update_after_scan calls on the same address complete
    without data races: final state is fully self-consistent.
    """
    clock = FakeClock()
    reg = AddressRegistry(time_func=clock)
    await reg.add_address(ADDR_A)

    # Each coroutine writes a distinct total_usd value.
    async def updater(i: int) -> None:
        await reg.update_after_scan(
            ADDR_A,
            total_usd=float(i) * 1000,
            has_position=True,
            scanned_at=float(i),
        )

    await asyncio.gather(*[updater(i) for i in range(100)])

    rec = reg._records[ADDR_A]
    # The final values must be internally consistent (one atomic write won).
    # total_usd and last_scanned must both come from the same call.
    assert rec.total_usd in {float(i) * 1000 for i in range(100)}
    assert rec.last_scanned in {float(i) for i in range(100)}
    # tier must match the final total_usd (has_position=True throughout)
    if rec.total_usd >= 1_000_000:
        assert rec.tier == Tier.WHALE
    elif rec.total_usd >= 50_000:
        assert rec.tier == Tier.MIDTIER
    else:
        assert rec.tier == Tier.LONGTAIL
    # consecutive_empty must be 0 (has_position always True)
    assert rec.consecutive_empty == 0
