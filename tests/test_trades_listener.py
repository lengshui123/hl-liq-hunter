"""
Tests for collector/trades_listener.py — TradesListener.
No real WebSocket connections.  websockets.connect is monkeypatched
with a MockWS async context manager.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from hl_liq_hunter.collector.trades_listener import TradesListener
from hl_liq_hunter.core.address_registry import AddressRegistry

# ── helpers ───────────────────────────────────────────────────────────────────


class MockWS:
    """Minimal async context manager that yields pre-canned WS messages."""

    def __init__(self, messages: list[str]) -> None:
        self._messages = list(messages)
        self.sent: list[str] = []

    async def __aenter__(self) -> "MockWS":
        return self

    async def __aexit__(self, *_: object) -> None:
        pass

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    def __aiter__(self) -> "MockWS":
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            # Block until cancelled so the shard loop doesn't spin.
            await asyncio.Event().wait()
        return self._messages.pop(0)


def _make_listener(
    symbols: list[str] | None = None,
    symbols_per_connection: int = 10,
) -> tuple[TradesListener, AsyncMock]:
    registry: AsyncMock = AsyncMock(spec=AddressRegistry)
    listener = TradesListener(
        registry=registry,
        symbols=symbols or ["BTC", "ETH"],
        symbols_per_connection=symbols_per_connection,
    )
    return listener, registry


def _trade_msg(users: list[str], coin: str = "BTC") -> str:
    return json.dumps({
        "channel": "trades",
        "data": [{
            "coin":  coin,
            "side":  "A",
            "px":    "65000.0",
            "sz":    "0.1",
            "time":  1779252000000,
            "hash":  "0x" + "0" * 64,
            "tid":   12345,
            "users": users,
        }],
    })


# ── sharding ──────────────────────────────────────────────────────────────────

def test_shard_splits_correctly() -> None:
    """30 symbols / shard-size 10 → 3 shards of 10 each."""
    symbols = [f"SYM{i}" for i in range(30)]
    listener, _ = _make_listener(symbols, symbols_per_connection=10)
    shards = listener._make_shards(symbols)
    assert len(shards) == 3
    assert all(len(s) == 10 for s in shards)
    assert [sym for shard in shards for sym in shard] == symbols


def test_shard_uneven_split() -> None:
    """25 symbols / shard-size 10 → 2 full + 1 partial."""
    symbols = [f"SYM{i}" for i in range(25)]
    listener, _ = _make_listener(symbols, symbols_per_connection=10)
    shards = listener._make_shards(symbols)
    assert len(shards) == 3
    assert len(shards[2]) == 5


# ── _handle_message ───────────────────────────────────────────────────────────

async def test_handle_message_valid_trades() -> None:
    """Valid trades message fires mark_dirty for each 0x address."""
    listener, registry = _make_listener()
    msg = _trade_msg(["0xAlice", "0xBob"])

    listener._handle_message(msg)

    # Two tasks were scheduled — drain the event loop so they run.
    await asyncio.sleep(0)
    registry.mark_dirty.assert_awaited()
    assert registry.mark_dirty.await_count == 2
    assert listener.stats()["trades_received"] == 1
    assert listener.stats()["addresses_marked"] == 2


async def test_handle_message_ignores_non_trades() -> None:
    """subscriptionResponse and other non-trades channels are silently ignored."""
    listener, registry = _make_listener()
    sub_response = json.dumps({
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}},
    })
    listener._handle_message(sub_response)
    await asyncio.sleep(0)

    registry.mark_dirty.assert_not_called()
    assert listener.stats()["trades_received"] == 0


async def test_handle_message_handles_invalid_json() -> None:
    """Malformed JSON increments parse_errors and does not crash."""
    listener, registry = _make_listener()
    listener._handle_message("{this is not json}")

    assert listener.stats()["parse_errors"] == 1
    registry.mark_dirty.assert_not_called()


async def test_handle_message_ignores_invalid_users_field() -> None:
    """trade with users not a list, or trade itself not a dict — no crash."""
    listener, registry = _make_listener()

    # users is a string (not a list)
    msg1 = json.dumps({"channel": "trades", "data": [
        {"coin": "BTC", "users": "not-a-list"},
    ]})
    # entire trade entry is a scalar
    msg2 = json.dumps({"channel": "trades", "data": [42]})

    listener._handle_message(msg1)
    listener._handle_message(msg2)
    await asyncio.sleep(0)

    registry.mark_dirty.assert_not_called()


async def test_handle_message_only_marks_0x_addresses() -> None:
    """Only users starting with '0x' are marked; others are silently skipped."""
    listener, registry = _make_listener()
    msg = _trade_msg(["0xValid", "not-an-address", "", "0xAlsoValid"])

    listener._handle_message(msg)
    await asyncio.sleep(0)

    assert registry.mark_dirty.await_count == 2
    marked = [call.args[0] for call in registry.mark_dirty.call_args_list]
    assert "0xValid" in marked
    assert "0xAlsoValid" in marked
    assert "not-an-address" not in marked


# ── _run_shard reconnect ──────────────────────────────────────────────────────

async def test_shard_reconnect_backoff() -> None:
    """
    Connection failures trigger exponential backoff up to the configured max.
    Verify reconnect_count increments and sleep durations grow correctly.
    """
    listener, _ = _make_listener()

    connect_calls = 0
    sleep_durations: list[float] = []

    class FailingConnect:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

        async def __aenter__(self) -> "FailingConnect":
            nonlocal connect_calls
            connect_calls += 1
            if connect_calls >= 4:
                raise asyncio.CancelledError
            raise ConnectionRefusedError("down")

        async def __aexit__(self, *_: object) -> None:
            pass

    async def fake_sleep(t: float) -> None:
        sleep_durations.append(t)

    with (
        patch("hl_liq_hunter.collector.trades_listener.websockets.connect", FailingConnect),
        patch("asyncio.sleep", fake_sleep),
    ):
        with pytest.raises(asyncio.CancelledError):
            await listener._run_shard(0, ["BTC"])

    assert listener.stats()["reconnect_count"] == 3
    # Backoff: 1.0, 2.0, 4.0
    assert sleep_durations == [1.0, 2.0, 4.0]


# ── run / cancel ──────────────────────────────────────────────────────────────

async def test_run_cancel_clean_exit() -> None:
    """Cancelling run() terminates all shards cleanly."""
    listener, _ = _make_listener(["BTC", "ETH", "SOL"], symbols_per_connection=2)

    mock_ws = MockWS([])

    with patch(
        "hl_liq_hunter.collector.trades_listener.websockets.connect",
        return_value=mock_ws,
    ):
        task = asyncio.create_task(listener.run())
        await asyncio.sleep(0.05)   # let shards connect
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
