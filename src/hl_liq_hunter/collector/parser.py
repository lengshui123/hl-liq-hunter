"""
Phase 2 — Module 1: clearinghouseState response parser.

Converts a raw API response dict into a structured ParseResult.
All numeric fields from the API are strings and must be float()-converted.
liquidationPx is a float (or null) in the API — not a string.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from hl_liq_hunter.config import PHASE2_SKIP_NULL_LIQ_PX

logger = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(value: str | float | int | None, default: float = 0.0) -> float:
    """Convert any numeric-ish value to float; return default on failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _parse_account_float(
    state: dict[str, object],
    key: str,
    subkey: str,
) -> float | None:
    """Extract a float from state[key][subkey]; return None on any failure."""
    try:
        section = state[key]
        if not isinstance(section, dict):
            return None
        raw = section.get(subkey)
        if raw is None:
            return None
        return float(raw)
    except (KeyError, TypeError, ValueError):
        return None


def _parse_top_float(state: dict[str, object], key: str) -> float | None:
    """Extract a float from state[key]; return None on any failure."""
    try:
        raw = state[key]
        if raw is None:
            return None
        return float(raw)  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError):
        return None


# ── result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParseResult:
    """Output of parse_positions for one address/response pair."""

    address: str
    timestamp_ms: int
    # One row per position that passed all filters (non-zero szi, non-null liq_px).
    rows: list[dict[str, object]]
    # Sum of positionValue for ALL non-zero positions (including null-liq ones).
    # Used for tier classification — must not exclude null positions.
    total_usd: float
    # True if any position has szi != 0.
    has_position: bool
    # Count of positions skipped because liquidationPx was null.
    null_liq_count: int
    # Symbols whose liq_px was null (for null-rate logging).
    null_liq_symbols: list[str]
    # Count of positions that raised an exception during parsing.
    parse_errors: int


# ── public API ────────────────────────────────────────────────────────────────

def parse_positions(
    address: str,
    state: dict[str, object],
    timestamp_ms: int,
    *,
    skip_null_liq_px: bool = PHASE2_SKIP_NULL_LIQ_PX,
) -> ParseResult:
    """
    Parse a clearinghouseState API response into a ParseResult.

    Parameters
    ----------
    address:
        The wallet address this response belongs to.
    state:
        The raw dict returned by HLClient.clearinghouse_state().
    timestamp_ms:
        Unix milliseconds at which the snapshot was taken (from caller).
    skip_null_liq_px:
        When True (default from config), positions with null liquidationPx are
        counted toward total_usd and has_position, but are NOT emitted as rows.
        When False, raises NotImplementedError (formula fallback out of scope).

    Returns
    -------
    ParseResult
    """
    if not skip_null_liq_px:
        raise NotImplementedError(
            "skip_null_liq_px=False (formula fallback) is not implemented. "
            "See docs/api_notes.md — null positions have near-zero density signal."
        )

    asset_positions = state.get("assetPositions")
    if not isinstance(asset_positions, list):
        return ParseResult(
            address=address,
            timestamp_ms=timestamp_ms,
            rows=[],
            total_usd=0.0,
            has_position=False,
            null_liq_count=0,
            null_liq_symbols=[],
            parse_errors=0,
        )

    rows: list[dict[str, object]] = []
    total_usd: float = 0.0
    has_position: bool = False
    null_liq_count: int = 0
    null_liq_symbols: list[str] = []
    parse_errors: int = 0

    for item in asset_positions:
        try:
            if not isinstance(item, dict):
                parse_errors += 1
                continue

            pos = item.get("position")
            if not isinstance(pos, dict):
                parse_errors += 1
                continue

            symbol: str = str(pos.get("coin", ""))

            # ── 1. szi check — skip zero-size positions ──────────────────────
            szi = _safe_float(pos.get("szi"), default=0.0)
            if szi == 0.0:
                continue

            has_position = True

            # ── 2. liq_px check — skip null (with accounting) ────────────────
            liq_px_raw = pos.get("liquidationPx")
            # positionValue is a STRING per HL API (confirmed in smoke test)
            position_value = _safe_float(pos.get("positionValue"), default=0.0)
            size_usd = abs(position_value)
            total_usd += size_usd

            if liq_px_raw is None:
                null_liq_count += 1
                null_liq_symbols.append(symbol)
                continue

            liq_px = float(liq_px_raw)

            # ── 3. leverage ───────────────────────────────────────────────────
            leverage_dict = pos.get("leverage")
            if not isinstance(leverage_dict, dict):
                parse_errors += 1
                continue

            lev_type: str = str(leverage_dict.get("type", ""))
            leverage = float(leverage_dict.get("value", 0))

            # ── 4. remaining fields ───────────────────────────────────────────
            side: str = "long" if szi > 0.0 else "short"
            entry_px = _safe_float(pos.get("entryPx"), default=0.0)

            # ── 5. emit row ───────────────────────────────────────────────────
            row: dict[str, object] = {
                "address":      address,
                "symbol":       symbol,
                "timestamp_ms": timestamp_ms,
                "side":         side,
                "size_usd":     size_usd,
                "entry_px":     entry_px,
                "liq_px":       liq_px,
                "leverage":     leverage,
                "lev_type":     lev_type,
            }
            rows.append(row)

        except Exception:
            parse_errors += 1
            logger.debug(
                "parse_positions: exception on position in address=%s symbol=%s",
                address,
                symbol if "symbol" in dir() else "?",
                exc_info=True,
            )

    return ParseResult(
        address=address,
        timestamp_ms=timestamp_ms,
        rows=rows,
        total_usd=total_usd,
        has_position=has_position,
        null_liq_count=null_liq_count,
        null_liq_symbols=null_liq_symbols,
        parse_errors=parse_errors,
    )
