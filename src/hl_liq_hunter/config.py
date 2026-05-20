"""
Central configuration for hl_liq_hunter.
All constants live here — no magic numbers in business logic.
Environment variables allow deployment-time overrides without code changes.
"""

from __future__ import annotations

import os
from enum import IntEnum
from pathlib import Path

# ── Filesystem paths ──────────────────────────────────────────────────────────
# Override via environment variables for server deployment (e.g. Aliyun).
DATA_ROOT: Path = Path(os.environ.get("HL_DATA_ROOT", "./data"))
LOG_DIR: Path = Path(os.environ.get("HL_LOG_DIR", "./logs"))

# ── HL API endpoints ──────────────────────────────────────────────────────────
HL_INFO_URL: str = "https://api.hyperliquid.xyz/info"
HL_WS_URL: str = "wss://api.hyperliquid.xyz/ws"

# ── Rate limiting (see docs/api_notes.md) ─────────────────────────────────────
HL_RATE_LIMIT: int = 1200   # hard limit: weight / min / IP
HL_RATE_BUDGET: int = 1000  # safe budget (~83 % of hard limit)

# Endpoint weights confirmed via Phase 0 testing (docs/api_notes.md).
# Added here so quota consumers don't embed magic numbers.
ENDPOINT_WEIGHTS: dict[str, int] = {
    "clearinghouseState": 2,   # confirmed weight=2 (NOT 1)
    "candleSnapshot":     20,  # unconfirmed — placeholder until measured
    "allMids":            2,   # unconfirmed — placeholder
}

# ── WebSocket sharding ────────────────────────────────────────────────────────
# HL drops connections with > ~12 active subscriptions (observed Phase 0).
WS_MAX_SUBS_PER_CONN: int = 10

# ── Address tier system ───────────────────────────────────────────────────────

class Tier(IntEnum):
    """
    Address classification tiers.

    IMPORTANT: Tier.DIRTY is a query-only sentinel value.
    AddressRecord.tier is NEVER set to Tier.DIRTY; dirtiness is tracked
    via the dirty_until timestamp field instead.
    """
    DIRTY    = 0  # sentinel for get_due_addresses exclusion only
    WHALE    = 1  # total_usd >= $1M
    MIDTIER  = 2  # total_usd >= $50k
    LONGTAIL = 3  # everything else (or no open position)


# Minimum USD position value to reach each named tier (inclusive).
TIER_THRESHOLDS_USD: dict[Tier, float] = {
    Tier.WHALE:   1_000_000.0,  # >= $1M  (== $1M counts as WHALE)
    Tier.MIDTIER:    50_000.0,  # >= $50k (== $50k counts as MIDTIER)
}

# How often each tier should be re-scanned (seconds).
# Tier.DIRTY interval is kept for completeness but is not used in
# get_due_addresses because no record stores Tier.DIRTY.
TIER_SCAN_INTERVAL_SEC: dict[Tier, int] = {
    Tier.DIRTY:      60,
    Tier.WHALE:     120,
    Tier.MIDTIER:   600,
    Tier.LONGTAIL: 3600,
}

# How long a dirty mark lasts (seconds).
DIRTY_SET_TTL_SEC: int = 300

# Minimum gap between two scans of the same dirty address (seconds).
DIRTY_MIN_RESCAN_SEC: int = 30

# ── Symbol scope ──────────────────────────────────────────────────────────────
# Phase 1 / 2 trial set.  Expand to 30+ symbols after Phase 2 collector stabilises.
SYMBOLS: list[str] = [
    "BTC", "ETH", "SOL", "HYPE", "BNB",
    "XRP", "DOGE", "ARB", "SUI", "AVAX",
]
