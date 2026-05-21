"""
Phase 2 — Module 4: TradesListener.

Subscribes to HL WebSocket trades channel across sharded connections,
extracts user addresses from each fill, and marks them dirty in
AddressRegistry so TierScanner re-scans them with higher priority.

Sharding: each WebSocket connection carries at most WS_SYMBOLS_PER_CONNECTION
subscriptions (HL drops connections with > ~12 active subs).  With 10 symbols
the default SYMBOLS list fits in one shard; with 30+ symbols it splits into
3 connections automatically.

Reconnect: each shard has its own exponential-backoff reconnect loop.
A failing shard does not affect other shards.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets
import websockets.exceptions

from hl_liq_hunter.config import (
    HL_WS_URL,
    WS_PING_INTERVAL_SEC,
    WS_RECONNECT_INITIAL_SEC,
    WS_RECONNECT_MAX_SEC,
    WS_RECONNECT_MULTIPLIER,
    WS_SYMBOLS_PER_CONNECTION,
)
from hl_liq_hunter.core.address_registry import AddressRegistry

logger = logging.getLogger(__name__)


class TradesListener:
    """
    Live WebSocket listener for HL trades.

    Lifecycle:
        listener = TradesListener(registry, symbols)
        task = asyncio.create_task(listener.run())
        # … later …
        task.cancel(); await task
    """

    def __init__(
        self,
        registry: AddressRegistry,
        symbols: list[str],
        *,
        ws_url: str = HL_WS_URL,
        symbols_per_connection: int = WS_SYMBOLS_PER_CONNECTION,
    ) -> None:
        self._registry = registry
        self._symbols = list(symbols)
        self._ws_url = ws_url
        self._symbols_per_conn = symbols_per_connection

        # Stats — updated from the event loop thread only; no lock needed.
        self._trades_received: int = 0
        self._addresses_marked: int = 0
        self._last_message_at: float = 0.0
        self._shards_connected: int = 0
        self._reconnect_count: int = 0
        self._parse_errors: int = 0

    # ── public API ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Start all shard coroutines concurrently.  Exits on CancelledError.
        Each shard reconnects independently; a single shard failure does not
        stop the others.
        """
        shards = self._make_shards(self._symbols)
        if not shards:
            logger.warning("TradesListener started with no symbols — idle")
            await asyncio.Event().wait()  # block until cancelled
            return

        async with asyncio.TaskGroup() as tg:
            for shard_id, shard_symbols in enumerate(shards):
                tg.create_task(
                    self._run_shard(shard_id, shard_symbols),
                    name=f"trades_shard_{shard_id}",
                )

    def stats(self) -> dict[str, object]:
        """Lock-free snapshot of listener state."""
        return {
            "trades_received":   self._trades_received,
            "addresses_marked":  self._addresses_marked,
            "last_message_at":   self._last_message_at,
            "shards_connected":  self._shards_connected,
            "reconnect_count":   self._reconnect_count,
            "parse_errors":      self._parse_errors,
        }

    # ── sharding ──────────────────────────────────────────────────────────────

    def _make_shards(self, symbols: list[str]) -> list[list[str]]:
        """Split symbols into chunks of at most symbols_per_conn."""
        n = self._symbols_per_conn
        return [symbols[i : i + n] for i in range(0, len(symbols), n)]

    # ── per-shard WS loop ─────────────────────────────────────────────────────

    async def _run_shard(self, shard_id: int, symbols: list[str]) -> None:
        """
        Maintain a WebSocket connection for one symbol shard.

        Reconnects with exponential backoff on any error.
        CancelledError propagates immediately.
        """
        reconnect_delay = WS_RECONNECT_INITIAL_SEC
        while True:
            try:
                logger.info(
                    "[shard %d] connecting, symbols=%s", shard_id, symbols
                )
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=WS_PING_INTERVAL_SEC,
                ) as ws:
                    for sym in symbols:
                        await ws.send(
                            json.dumps({
                                "method": "subscribe",
                                "subscription": {"type": "trades", "coin": sym},
                            })
                        )

                    # Connection is live — reset backoff and count the shard.
                    reconnect_delay = WS_RECONNECT_INITIAL_SEC
                    self._shards_connected += 1

                    async for raw_msg in ws:
                        self._handle_message(raw_msg)

            except asyncio.CancelledError:
                logger.info("[shard %d] cancelled", shard_id)
                raise
            except Exception as exc:
                self._reconnect_count += 1
                logger.warning(
                    "[shard %d] disconnected: %s — reconnecting in %.1fs",
                    shard_id, exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * WS_RECONNECT_MULTIPLIER,
                    WS_RECONNECT_MAX_SEC,
                )
            finally:
                # Decrement regardless of how we left the try block.
                # guard against going negative on first-connect failures.
                self._shards_connected = max(0, self._shards_connected - 1)

    # ── message handling ──────────────────────────────────────────────────────

    def _handle_message(self, raw_msg: str | bytes) -> None:
        """
        Parse one raw WS frame and fire mark_dirty tasks for all user addresses.

        Synchronous by design — called from the async-for loop in _run_shard.
        mark_dirty is async, so each call is scheduled as a fire-and-forget
        asyncio.Task.  The stats counter _addresses_marked is incremented on
        task creation, not task completion; a tiny undercount can occur only
        during shutdown when queued tasks are cancelled.

        Non-trades messages (subscriptionResponse, etc.) are silently ignored.
        """
        try:
            msg = json.loads(raw_msg)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._parse_errors += 1
            logger.debug("WS parse error on raw_msg=%r", raw_msg)
            return

        if not isinstance(msg, dict) or msg.get("channel") != "trades":
            return

        trades = msg.get("data", [])
        if not isinstance(trades, list):
            return

        for trade in trades:
            if not isinstance(trade, dict):
                continue
            users = trade.get("users", [])
            if not isinstance(users, list):
                continue
            for user in users:
                if isinstance(user, str) and user.startswith("0x"):
                    asyncio.create_task(
                        self._registry.mark_dirty(user),
                        name=f"mark_dirty_{user[:8]}",
                    )
                    self._addresses_marked += 1
            self._trades_received += 1

        self._last_message_at = time.time()
