"""
Tests for collector/scanner.py — TierScanner.
No network I/O.  HLClient and AddressRegistry are mocked via AsyncMock.
SnapshotWriter uses a real instance backed by tmp_path.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hl_liq_hunter.collector.scanner import TierScanner
from hl_liq_hunter.collector.writer import SnapshotWriter
from hl_liq_hunter.config import Tier
from hl_liq_hunter.core.address_registry import AddressRegistry
from hl_liq_hunter.core.hl_client import HLClient

# ── shared fixtures / helpers ─────────────────────────────────────────────────

# Minimal clearinghouseState response with one valid cross-long position.
_STATE_ONE_POSITION: dict[str, object] = {
    "assetPositions": [
        {
            "type": "oneWay",
            "position": {
                "coin": "BTC",
                "szi": "1.0",
                "leverage": {"type": "cross", "value": 5},
                "entryPx": "65000.0",
                "positionValue": "65000.0",
                "unrealizedPnl": "0.0",
                "returnOnEquity": "0.0",
                "liquidationPx": "60000.0",
                "marginUsed": "13000.0",
                "maxLeverage": 10,
                "cumFunding": {"allTime": "0.0", "sinceOpen": "0.0", "sinceChange": "0.0"},
            },
        }
    ],
    "time": 1779252000000,
}

_STATE_EMPTY: dict[str, object] = {"assetPositions": [], "time": 1779252000000}

_ADDR = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _make(
    tmp_path: Path,
    tier: Tier = Tier.WHALE,
    **kwargs: Any,
) -> tuple[TierScanner, AsyncMock, AsyncMock, SnapshotWriter]:
    """Return (scanner, mock_client, mock_registry, real_writer)."""
    client: AsyncMock = AsyncMock(spec=HLClient)
    registry: AsyncMock = AsyncMock(spec=AddressRegistry)
    writer = SnapshotWriter(data_root=tmp_path, flush_interval_sec=60.0)
    scanner = TierScanner(
        tier=tier,
        client=client,
        registry=registry,
        writer=writer,
        **kwargs,
    )
    return scanner, client, registry, writer


# ── scan_one ──────────────────────────────────────────────────────────────────

async def test_scan_one_success(tmp_path: Path) -> None:
    """Success path: registry is updated and rows land in the writer buffer."""
    scanner, client, registry, writer = _make(tmp_path)
    client.clearinghouse_state.return_value = _STATE_ONE_POSITION

    await scanner.scan_one(_ADDR)

    registry.update_after_scan.assert_called_once()
    args = registry.update_after_scan.call_args
    assert args.args[0] == _ADDR           # address
    assert args.args[2] is True            # has_position
    assert args.args[1] > 0.0             # total_usd

    assert writer.stats()["buffer_size"] == 1
    assert scanner.stats()["scans_completed"] == 1
    assert scanner.stats()["scans_failed"] == 0


async def test_scan_one_none_skip_update(tmp_path: Path) -> None:
    """None response: update_after_scan must NOT be called (last_scanned unchanged)."""
    scanner, client, registry, _ = _make(tmp_path)
    client.clearinghouse_state.return_value = None

    await scanner.scan_one(_ADDR)

    registry.update_after_scan.assert_not_called()
    assert scanner.stats()["scans_completed"] == 0
    assert scanner.stats()["scans_failed"] == 0


async def test_scan_one_empty_positions(tmp_path: Path) -> None:
    """Empty assetPositions: registry updated with has_position=False, no rows written."""
    scanner, client, registry, writer = _make(tmp_path)
    client.clearinghouse_state.return_value = _STATE_EMPTY

    await scanner.scan_one(_ADDR)

    registry.update_after_scan.assert_called_once()
    args = registry.update_after_scan.call_args
    assert args.args[2] is False           # has_position=False
    assert args.args[1] == 0.0            # total_usd=0

    assert writer.stats()["buffer_size"] == 0  # no rows written
    assert scanner.stats()["scans_completed"] == 1


async def test_scan_one_exception_caught(tmp_path: Path) -> None:
    """Exception from clearinghouse_state: scan_one must not raise; scans_failed += 1."""
    scanner, client, registry, _ = _make(tmp_path)
    client.clearinghouse_state.side_effect = RuntimeError("network error")

    # Must not raise
    await scanner.scan_one(_ADDR)

    registry.update_after_scan.assert_not_called()
    assert scanner.stats()["scans_failed"] == 1
    assert scanner.stats()["scans_completed"] == 0


# ── scan_batch ────────────────────────────────────────────────────────────────

async def test_scan_batch_concurrency(tmp_path: Path) -> None:
    """At most `concurrency` scan_one calls run simultaneously."""
    scanner, _, _, _ = _make(tmp_path, concurrency=5)

    concurrent = 0
    max_concurrent = 0

    async def tracked(addr: str) -> None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        await asyncio.sleep(0.02)          # hold slot long enough for others to pile up
        concurrent -= 1

    scanner.scan_one = tracked             # type: ignore[assignment]

    addresses = [f"0x{i:040x}" for i in range(20)]
    await scanner.scan_batch(addresses)

    assert max_concurrent <= 5
    assert max_concurrent >= 2             # confirmed actual concurrency


async def test_scan_batch_inter_batch_sleep(tmp_path: Path) -> None:
    """scan_batch sleeps inter_batch_sleep_sec after the concurrent phase."""
    scanner, _, _, _ = _make(tmp_path, inter_batch_sleep_sec=0.05)

    async def instant(_addr: str) -> None:
        pass

    scanner.scan_one = instant             # type: ignore[assignment]

    t0 = time.monotonic()
    await scanner.scan_batch([_ADDR])
    elapsed = time.monotonic() - t0

    assert elapsed >= 0.05


async def test_scan_batch_updates_stats(tmp_path: Path) -> None:
    """last_batch_size and last_batch_at are recorded after scan_batch."""
    scanner, client, registry, _ = _make(tmp_path)
    client.clearinghouse_state.return_value = _STATE_EMPTY

    t0 = time.time()
    await scanner.scan_batch([_ADDR, _ADDR])
    s = scanner.stats()

    assert s["last_batch_size"] == 2
    assert float(s["last_batch_at"]) >= t0  # type: ignore[arg-type]


# ── run loop ──────────────────────────────────────────────────────────────────

async def test_run_no_due_addresses_idle_sleep(tmp_path: Path) -> None:
    """When no addresses are due, run() loops in idle sleep."""
    scanner, client, registry, _ = _make(tmp_path, tier=Tier.WHALE, idle_sleep_sec=0.01)
    client.clearinghouse_state.return_value = None

    call_count = 0

    async def get_due(tier: Tier, limit: int) -> list[str]:
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            raise asyncio.CancelledError
        return []

    registry.get_due_addresses.side_effect = get_due

    with pytest.raises(asyncio.CancelledError):
        await scanner.run()

    assert call_count >= 3                 # looped back after empty result


async def test_run_cancelled(tmp_path: Path) -> None:
    """Cancelling the run task exits cleanly via CancelledError."""
    scanner, _, registry, _ = _make(tmp_path, tier=Tier.WHALE, idle_sleep_sec=0.01)
    registry.get_due_addresses.return_value = []

    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_dirty_tier_uses_get_dirty(tmp_path: Path) -> None:
    """DIRTY tier calls get_dirty_addresses, never get_due_addresses."""
    scanner, _, registry, _ = _make(tmp_path, tier=Tier.DIRTY)

    async def get_dirty(limit: int) -> list[str]:
        raise asyncio.CancelledError

    registry.get_dirty_addresses.side_effect = get_dirty

    with pytest.raises(asyncio.CancelledError):
        await scanner.run()

    registry.get_dirty_addresses.assert_called_once()
    registry.get_due_addresses.assert_not_called()


async def test_run_other_tier_uses_get_due(tmp_path: Path) -> None:
    """Non-DIRTY tiers call get_due_addresses with the correct tier argument."""
    scanner, _, registry, _ = _make(tmp_path, tier=Tier.MIDTIER, batch_size=25)

    async def get_due(tier: Tier, limit: int) -> list[str]:
        raise asyncio.CancelledError

    registry.get_due_addresses.side_effect = get_due

    with pytest.raises(asyncio.CancelledError):
        await scanner.run()

    registry.get_due_addresses.assert_called_once_with(Tier.MIDTIER, 25)
    registry.get_dirty_addresses.assert_not_called()


# ── stats ─────────────────────────────────────────────────────────────────────

async def test_stats_increments(tmp_path: Path) -> None:
    """scans_completed and scans_failed accumulate correctly across multiple scan_one calls."""
    scanner, client, registry, _ = _make(tmp_path)

    # Three successful scans
    client.clearinghouse_state.return_value = _STATE_ONE_POSITION
    for _ in range(3):
        await scanner.scan_one(_ADDR)

    # One failure
    client.clearinghouse_state.side_effect = ValueError("parse error")
    await scanner.scan_one(_ADDR)

    s = scanner.stats()
    assert s["scans_completed"] == 3
    assert s["scans_failed"] == 1
    assert s["null_liq_px_total"] == 0     # _STATE_ONE_POSITION has no null liq_px
