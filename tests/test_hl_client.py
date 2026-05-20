"""
Tests for HLClient.

Uses aioresponses to mock aiohttp.ClientSession without touching the network.
A real QuotaManager (with budget=10000) is injected so quota assertions work
without any sleeps.
"""

from __future__ import annotations

import asyncio

import pytest
from aioresponses import aioresponses

from hl_liq_hunter.config import HL_INFO_URL
from hl_liq_hunter.core.hl_client import HLClient
from hl_liq_hunter.core.quota_manager import QuotaManager

# ── Shared fixtures ───────────────────────────────────────────────────────────

ADDR = "0x" + "a" * 40


def make_quota(max_per_min: int = 10_000) -> QuotaManager:
    """Large budget so tests never block on quota."""
    return QuotaManager(max_per_min=max_per_min)


# ── 1. clearinghouse_state success ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_clearinghouse_state_success() -> None:
    """200 with valid JSON returns the parsed dict."""
    payload = {"assetPositions": [], "crossMarginSummary": {"accountValue": "1234.5"}}
    quota = make_quota()

    with aioresponses() as mock:
        mock.post(HL_INFO_URL, payload=payload, status=200)
        async with HLClient(quota=quota) as client:
            result = await client.clearinghouse_state(ADDR)

    assert result == payload


# ── 2. clearinghouse_state 429 ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_clearinghouse_state_429(caplog: pytest.LogCaptureFixture) -> None:
    """429 → returns None, logs a warning, sleeps ~8s (patched to near-zero)."""
    import hl_liq_hunter.core.hl_client as _mod

    original = _mod._RATE_LIMIT_SLEEP_S
    _mod._RATE_LIMIT_SLEEP_S = 0.0  # avoid real 8-second sleep in tests
    try:
        quota = make_quota()
        with aioresponses() as mock:
            mock.post(HL_INFO_URL, status=429, body=b"Too Many Requests")
            with caplog.at_level("WARNING", logger="hl_liq_hunter.core.hl_client"):
                async with HLClient(quota=quota) as client:
                    result = await client.clearinghouse_state(ADDR)
    finally:
        _mod._RATE_LIMIT_SLEEP_S = original

    assert result is None
    assert any("rate limited" in r.message for r in caplog.records)


# ── 3. clearinghouse_state 5xx ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_clearinghouse_state_5xx(caplog: pytest.LogCaptureFixture) -> None:
    """503 → returns None, logs an error."""
    quota = make_quota()
    with aioresponses() as mock:
        mock.post(HL_INFO_URL, status=503, body=b"Service Unavailable")
        with caplog.at_level("ERROR", logger="hl_liq_hunter.core.hl_client"):
            async with HLClient(quota=quota) as client:
                result = await client.clearinghouse_state(ADDR)

    assert result is None
    assert any("503" in r.message for r in caplog.records)


# ── 4. clearinghouse_state timeout ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_clearinghouse_state_timeout(caplog: pytest.LogCaptureFixture) -> None:
    """asyncio.TimeoutError → returns None, logs at DEBUG."""
    import aiohttp

    quota = make_quota()
    with aioresponses() as mock:
        mock.post(HL_INFO_URL, exception=asyncio.TimeoutError())
        with caplog.at_level("DEBUG", logger="hl_liq_hunter.core.hl_client"):
            async with HLClient(quota=quota) as client:
                result = await client.clearinghouse_state(ADDR)

    assert result is None
    assert any("timed out" in r.message for r in caplog.records)


# ── 5. clearinghouse_state invalid JSON ───────────────────────────────────────

@pytest.mark.asyncio
async def test_clearinghouse_state_invalid_json(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """200 with non-JSON body → returns None, logs an error."""
    quota = make_quota()
    with aioresponses() as mock:
        mock.post(HL_INFO_URL, status=200, body=b"this is not json {{{{")
        with caplog.at_level("ERROR", logger="hl_liq_hunter.core.hl_client"):
            async with HLClient(quota=quota) as client:
                result = await client.clearinghouse_state(ADDR)

    assert result is None
    assert any("JSON decode failed" in r.message for r in caplog.records)


# ── 6. quota consumed ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_quota_consumed() -> None:
    """clearinghouse_state consumes exactly weight=2 from the quota."""
    quota = make_quota()
    payload = {"assetPositions": []}

    with aioresponses() as mock:
        mock.post(HL_INFO_URL, payload=payload, status=200)
        async with HLClient(quota=quota) as client:
            before = quota.stats()["usage_per_min"]
            await client.clearinghouse_state(ADDR)
            after = quota.stats()["usage_per_min"]

    assert after - before == 2


# ── 7. context manager lifecycle ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_context_manager_lifecycle() -> None:
    """Session is None before enter, open inside, closed after exit."""
    quota = make_quota()
    client = HLClient(quota=quota)

    assert client._session is None

    async with client:
        assert client._session is not None
        assert not client._session.closed

    assert client._session is None  # __aexit__ sets it to None after closing


# ── 8. all_mids success + float conversion ────────────────────────────────────

@pytest.mark.asyncio
async def test_all_mids_success() -> None:
    """allMids returns dict[str, float]; HL string prices are converted."""
    quota = make_quota()
    raw = {"BTC": "65000.5", "ETH": "3200.0", "SOL": "145.75"}

    with aioresponses() as mock:
        mock.post(HL_INFO_URL, payload=raw, status=200)
        async with HLClient(quota=quota) as client:
            result = await client.all_mids()

    assert result == {"BTC": 65000.5, "ETH": 3200.0, "SOL": 145.75}
    assert all(isinstance(v, float) for v in (result or {}).values())


# ── 9. candle_snapshot success ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_candle_snapshot_success() -> None:
    """candleSnapshot returns list; quota consumed weight=20."""
    quota = make_quota()
    candles = [
        {"t": 1_700_000_000_000, "o": "65000", "h": "65500", "l": "64800", "c": "65200", "v": "10.5", "n": 42}
    ]

    with aioresponses() as mock:
        mock.post(HL_INFO_URL, payload=candles, status=200)
        async with HLClient(quota=quota) as client:
            before = quota.stats()["usage_per_min"]
            result = await client.candle_snapshot("BTC", "1m", 1_700_000_000_000, 1_700_000_060_000)
            after = quota.stats()["usage_per_min"]

    assert result == candles
    assert after - before == 20
