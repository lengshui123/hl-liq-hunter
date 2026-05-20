"""
Tests for collector/parser.py — parse_positions function.
No network I/O.  All state dicts are either loaded from fixtures or built inline.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from hl_liq_hunter.collector.parser import ParseResult, parse_positions

FIXTURES = Path(__file__).parent / "fixtures"

ADDRESS_1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDRESS_2 = "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae"
ADDRESS_3 = "0xbdfa4f4492dd7b7cf211209c4791af8d52bf5c50"
TS = 1_779_252_000_000


def _load(name: str) -> dict[str, object]:
    raw: dict[str, object] = json.loads((FIXTURES / name).read_text())
    return raw


# ── fixture_1: synthetic isolated ETH long ────────────────────────────────────

def test_isolated_long() -> None:
    """Single isolated-long position with a real liq_px produces one row."""
    state = _load("clearinghouse_state_1.json")
    result = parse_positions(ADDRESS_1, state, TS)

    assert isinstance(result, ParseResult)
    assert result.address == ADDRESS_1
    assert result.timestamp_ms == TS
    assert len(result.rows) == 1
    assert result.has_position is True
    assert result.null_liq_count == 0
    assert result.null_liq_symbols == []
    assert result.parse_errors == 0

    row = result.rows[0]
    assert row["symbol"] == "ETH"
    assert row["side"] == "long"
    assert math.isclose(row["size_usd"], 9150.0, rel_tol=1e-6)  # type: ignore[arg-type]
    assert math.isclose(row["liq_px"], 2750.0, rel_tol=1e-6)  # type: ignore[arg-type]
    assert row["lev_type"] == "isolated"
    assert math.isclose(row["leverage"], 10.0, rel_tol=1e-6)  # type: ignore[arg-type]
    assert row["address"] == ADDRESS_1
    assert row["timestamp_ms"] == TS


# ── fixture_2: 6 cross positions, 2 null (LIT, AZTEC) ────────────────────────

def test_cross_null_skipped() -> None:
    """Null-liq positions are skipped; only non-null ones appear in rows."""
    state = _load("clearinghouse_state_2.json")
    result = parse_positions(ADDRESS_2, state, TS)

    assert len(result.rows) == 4  # TON, SAGA, HYPE, PAXG
    assert result.null_liq_count == 2
    assert set(result.null_liq_symbols) == {"LIT", "AZTEC"}
    assert result.parse_errors == 0

    symbols_in_rows = {r["symbol"] for r in result.rows}
    assert symbols_in_rows == {"TON", "SAGA", "HYPE", "PAXG"}


def test_cross_null_total_usd_includes_null() -> None:
    """total_usd must include ALL non-zero positions, even null-liq ones."""
    state = _load("clearinghouse_state_2.json")
    result = parse_positions(ADDRESS_2, state, TS)

    # Sum of all 6 positionValues from fixture_2
    expected = (
        67_557.8637
        + 198.845724
        + 70_774_078.3105700016
        + 473.2476
        + 1_816_712.7744
        + 5.511168
    )
    assert math.isclose(result.total_usd, expected, rel_tol=1e-6)
    assert result.has_position is True


def test_short_side_flag() -> None:
    """Negative szi produces side='short'."""
    state = _load("clearinghouse_state_2.json")
    result = parse_positions(ADDRESS_2, state, TS)

    # TON has szi="-34805.7" → short
    ton_rows = [r for r in result.rows if r["symbol"] == "TON"]
    assert len(ton_rows) == 1
    assert ton_rows[0]["side"] == "short"


# ── fixture_3: 2 cross positions, 1 null (MEGA) ───────────────────────────────

def test_fixture3_one_null_one_row() -> None:
    """fixture_3: 2 positions, 1 null → 1 row emitted, 1 null counted."""
    state = _load("clearinghouse_state_3.json")
    result = parse_positions(ADDRESS_3, state, TS)

    assert len(result.rows) == 1
    assert result.rows[0]["symbol"] == "VVV"
    assert result.null_liq_count == 1
    assert result.null_liq_symbols == ["MEGA"]
    assert result.has_position is True

    expected_total = 51_481.47315 + 105_388.701799
    assert math.isclose(result.total_usd, expected_total, rel_tol=1e-6)


# ── inline edge-case tests ────────────────────────────────────────────────────

def test_no_positions_empty_list() -> None:
    """assetPositions=[] → all zeros/falsy, no rows."""
    state: dict[str, object] = {"assetPositions": [], "time": TS}
    result = parse_positions(ADDRESS_1, state, TS)

    assert result.rows == []
    assert result.total_usd == 0.0
    assert result.has_position is False
    assert result.null_liq_count == 0
    assert result.parse_errors == 0


def test_skip_zero_szi() -> None:
    """Position with szi='0' is silently skipped — not an error, not a row."""
    state: dict[str, object] = {
        "assetPositions": [
            {
                "type": "oneWay",
                "position": {
                    "coin": "BTC",
                    "szi": "0",
                    "leverage": {"type": "cross", "value": 5},
                    "entryPx": "50000.0",
                    "positionValue": "0.0",
                    "unrealizedPnl": "0.0",
                    "returnOnEquity": "0.0",
                    "liquidationPx": None,
                    "marginUsed": "0.0",
                    "maxLeverage": 20,
                    "cumFunding": {},
                },
            }
        ],
        "time": TS,
    }
    result = parse_positions(ADDRESS_1, state, TS)

    assert result.rows == []
    assert result.has_position is False
    assert result.null_liq_count == 0
    assert result.total_usd == 0.0


def test_has_position_true_when_all_null_liq() -> None:
    """has_position=True even if every non-zero position has null liq_px."""
    state: dict[str, object] = {
        "assetPositions": [
            {
                "type": "oneWay",
                "position": {
                    "coin": "SOL",
                    "szi": "100.0",
                    "leverage": {"type": "cross", "value": 3},
                    "entryPx": "150.0",
                    "positionValue": "15000.0",
                    "unrealizedPnl": "0.0",
                    "returnOnEquity": "0.0",
                    "liquidationPx": None,
                    "marginUsed": "5000.0",
                    "maxLeverage": 10,
                    "cumFunding": {},
                },
            }
        ],
        "time": TS,
    }
    result = parse_positions(ADDRESS_1, state, TS)

    assert result.has_position is True
    assert result.rows == []
    assert result.null_liq_count == 1
    assert math.isclose(result.total_usd, 15000.0, rel_tol=1e-6)


def test_skip_null_false_raises() -> None:
    """skip_null_liq_px=False is not implemented and must raise."""
    state: dict[str, object] = {"assetPositions": []}
    with pytest.raises(NotImplementedError):
        parse_positions(ADDRESS_1, state, TS, skip_null_liq_px=False)


def test_parse_errors_on_malformed_item() -> None:
    """A non-dict item in assetPositions increments parse_errors, not a crash."""
    state: dict[str, object] = {
        "assetPositions": [
            "this-is-not-a-dict",
            {
                "type": "oneWay",
                "position": {
                    "coin": "BTC",
                    "szi": "1.0",
                    "leverage": {"type": "isolated", "value": 5},
                    "entryPx": "50000.0",
                    "positionValue": "50000.0",
                    "unrealizedPnl": "0.0",
                    "returnOnEquity": "0.0",
                    "liquidationPx": 45000.0,
                    "marginUsed": "10000.0",
                    "maxLeverage": 10,
                    "cumFunding": {},
                },
            },
        ],
        "time": TS,
    }
    result = parse_positions(ADDRESS_1, state, TS)

    assert result.parse_errors == 1
    assert len(result.rows) == 1
    assert result.rows[0]["symbol"] == "BTC"
