#!/usr/bin/env python3
"""
Collect active trader addresses via HL trades WebSocket.
Listens for 30 minutes across 23 symbols, extracts unique addresses,
appends to scripts/manual_addresses.txt.

Run: python scripts/collect_active_addresses.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("addr_collector")

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
# Only symbols confirmed stable in the previous 5-min run.
# k-prefixed and other uncertain names dropped — HL closes the connection on bad subscriptions.
SYMBOLS = [
    "BTC", "ETH", "SOL", "HYPE", "ARB", "DOGE",
    "AVAX", "SUI", "BNB", "XRP", "LINK", "AAVE",
]
COLLECT_SECONDS = 1800  # 30 minutes
MANUAL_FILE = Path("scripts/manual_addresses.txt")

ETH_RE = re.compile(r'^0x[a-fA-F0-9]{40}$')


def _extract_addresses_from_trade(trade: object) -> list[str]:
    """
    Pull all Ethereum addresses out of a single trade object.
    Field names are uncertain; we check every string value recursively.
    """
    found: list[str] = []
    if isinstance(trade, dict):
        for v in trade.values():
            found.extend(_extract_addresses_from_trade(v))
    elif isinstance(trade, list):
        for item in trade:
            found.extend(_extract_addresses_from_trade(item))
    elif isinstance(trade, str) and ETH_RE.match(trade):
        found.append(trade)
    return found


async def _listen_one_connection(
    ws: object,
    collected: set[str],
    trade_counts: dict[str, int],
    deadline: float,
    first_raw_printed: list[bool],
) -> int:
    """
    Drive a single WS connection until deadline or disconnect.
    Returns number of trades received this session.
    """
    total = 0
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5.0))  # type: ignore[attr-defined]
        except asyncio.TimeoutError:
            continue

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        channel = msg.get("channel", "")

        if not first_raw_printed[0] and channel == "trades":
            print("\n" + "="*70)
            print("FIRST TRADES MESSAGE — raw JSON (schema reference):")
            print("="*70)
            print(json.dumps(msg, indent=2))
            print("="*70 + "\n")
            first_raw_printed[0] = True

        if channel != "trades":
            continue

        data = msg.get("data", [])
        trades = data if isinstance(data, list) else [data]
        for trade in trades:
            coin = trade.get("coin", "UNKNOWN") if isinstance(trade, dict) else "UNKNOWN"
            trade_counts[coin] += 1
            total += 1
            for addr in _extract_addresses_from_trade(trade):
                collected.add(addr)

    return total


MAX_SYMS_PER_CONN = 10   # HL drops connections with > ~12 subscriptions

async def _run_shard(
    shard_id: int,
    syms: list[str],
    collected: set[str],
    trade_counts: dict[str, int],
    deadline: float,
    first_raw_printed: list[bool],
    total_trades_ref: list[int],
) -> None:
    """Keep one WS connection alive for a symbol shard until deadline."""
    backoff = 2.0
    reconnects = 0

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            async with websockets.connect(
                HL_WS_URL,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=5,
                open_timeout=15,
            ) as ws:
                for sym in syms:
                    await ws.send(json.dumps(
                        {"method": "subscribe", "subscription": {"type": "trades", "coin": sym}}
                    ))
                if reconnects == 0:
                    log.info(f"[shard {shard_id}] Connected: {syms}")
                else:
                    log.info(f"[shard {shard_id}] Reconnected (#{reconnects}): {syms}")
                backoff = 2.0

                n = await _listen_one_connection(
                    ws, collected, trade_counts, deadline, first_raw_printed
                )
                total_trades_ref[0] += n

        except (websockets.exceptions.ConnectionClosedError, Exception) as exc:
            elapsed = COLLECT_SECONDS - (deadline - time.monotonic())
            log.warning(
                f"[shard {shard_id}] closed at {elapsed:.0f}s "
                f"({len(collected)} addrs): {exc}. "
                f"Retry in {backoff:.1f}s"
            )
            reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 20.0)

    log.info(f"[shard {shard_id}] done.")


async def run() -> None:
    collected: set[str] = set()
    trade_counts: dict[str, int] = defaultdict(int)
    total_trades_ref = [0]
    first_raw_printed = [False]
    deadline = time.monotonic() + COLLECT_SECONDS

    # Split symbols into shards of ≤ MAX_SYMS_PER_CONN each
    shards = [SYMBOLS[i:i + MAX_SYMS_PER_CONN] for i in range(0, len(SYMBOLS), MAX_SYMS_PER_CONN)]
    log.info(
        f"{len(SYMBOLS)} symbols → {len(shards)} WS connections "
        f"(≤{MAX_SYMS_PER_CONN} syms each), {COLLECT_SECONDS}s"
    )

    await asyncio.gather(*[
        _run_shard(i, shard, collected, trade_counts, deadline, first_raw_printed, total_trades_ref)
        for i, shard in enumerate(shards)
    ])

    total_trades = total_trades_ref[0]
    log.info(
        f"Collection window closed. "
        f"Total trades: {total_trades}, "
        f"Unique addresses: {len(collected)}"
    )

    # ── Load existing addresses ──────────────────────────────────────────
    existing: set[str] = set()
    if MANUAL_FILE.exists():
        for line in MANUAL_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and ETH_RE.match(line):
                existing.add(line)

    new_addrs = collected - existing
    log.info(f"Existing addresses in file : {len(existing)}")
    log.info(f"Newly collected addresses  : {len(collected)}")
    log.info(f"Net new (not already there): {len(new_addrs)}")

    # ── Append new addresses ─────────────────────────────────────────────
    if new_addrs:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        with MANUAL_FILE.open("a") as f:
            f.write(f"\n# --- collected via WS {ts} ---\n")
            for addr in sorted(new_addrs):
                f.write(addr + "\n")
        log.info(f"Appended {len(new_addrs)} addresses to {MANUAL_FILE}")
    else:
        log.info("No new addresses to append.")

    total_in_file = len(existing) + len(new_addrs)

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("COLLECTION SUMMARY")
    print("="*70)
    print(f"  Duration         : {COLLECT_SECONDS}s")
    print(f"  Total trades     : {total_trades}")
    print(f"  Unique addresses : {len(collected)}")
    print(f"    Already in file: {len(existing & collected)}")
    print(f"    Net new        : {len(new_addrs)}")
    print(f"  Total in file now: {total_in_file}")
    print()
    print("  Trade count by symbol:")
    for sym in sorted(trade_counts, key=trade_counts.get, reverse=True):  # type: ignore[arg-type]
        print(f"    {sym:<6} {trade_counts[sym]:>5} trades")
    print("="*70)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        sys.exit(0)
