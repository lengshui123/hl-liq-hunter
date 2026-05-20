#!/usr/bin/env python3
"""
Phase 0 Feasibility Check — HL Liquidation Hunter
===================================================
One-shot script: snapshot current liquidation density for BTC/ETH,
overlay on 24h price action, and see if there is any visual correlation.

Run:
    cd hl_liq_hunter
    python scripts/feasibility_check.py

Output:
    output/feasibility_<SYMBOL>_<TIMESTAMP>.png
    Text report printed to stdout
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

SYMBOLS = ["BTC", "ETH"]
MAX_ADDRESSES = 3000
SEMAPHORE_LIMIT = 20
SLEEP_BETWEEN_CALLS = 0.1       # seconds – prevents 429s

PRICE_RANGE_PCT    = 0.05       # ±5% around current price
FAR_THRESHOLD_PCT  = 0.02       # boundary between "near" and "far" zones
FAR_BIN_PCT        = 0.005      # 0.5% bins in far zone
NEAR_BIN_PCT       = 0.001      # 0.1% bins in near zone
CLUSTER_USD        = 20_000_000 # $20M threshold for "major cluster"

OUTPUT_DIR = Path("output")
MANUAL_ADDRS_FILE = Path("scripts/manual_addresses.txt")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("feasibility")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Address collection (three-tier fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _pluck_addresses(obj: object, depth: int = 0) -> list[str]:
    """Recursively pull 0x… Ethereum addresses out of any JSON structure."""
    if depth > 10:
        return []
    if isinstance(obj, str):
        return [obj] if (len(obj) == 42 and obj.startswith("0x")) else []
    if isinstance(obj, dict):
        out: list[str] = []
        for v in obj.values():
            out.extend(_pluck_addresses(v, depth + 1))
            if len(out) >= MAX_ADDRESSES:
                break
        return out
    if isinstance(obj, list):
        out = []
        for item in obj:
            out.extend(_pluck_addresses(item, depth + 1))
            if len(out) >= MAX_ADDRESSES:
                break
        return out
    return []


async def _try_hypurrscan(session: aiohttp.ClientSession) -> list[str]:
    """
    Attempt A: scrape Hypurrscan leaderboard.
    Tries the JSON API first, then falls back to HTML + __NEXT_DATA__ parsing.
    """
    browser_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    timeout = aiohttp.ClientTimeout(total=15)

    # Possible JSON API endpoints
    api_candidates = [
        "https://hypurrscan.io/api/leaderboard",
        "https://api.hypurrscan.io/leaderboard",
        "https://hypurrscan.io/api/traders",
        "https://api.hypurrscan.io/v1/leaderboard",
    ]
    for url in api_candidates:
        try:
            async with session.get(url, headers=browser_headers, timeout=timeout) as r:
                if r.status == 200:
                    ct = r.headers.get("Content-Type", "")
                    if "json" in ct:
                        data = await r.json(content_type=None)
                        addrs = list(dict.fromkeys(_pluck_addresses(data)))
                        if len(addrs) >= 20:
                            log.info(f"Hypurrscan JSON API ({url}): {len(addrs)} addresses")
                            return addrs
        except Exception as exc:
            log.debug(f"Hypurrscan API {url}: {exc}")

    # Try the HTML page — addresses are sometimes embedded in __NEXT_DATA__
    try:
        async with session.get(
            "https://hypurrscan.io/leaderboard",
            headers=browser_headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 200:
                html = await r.text()
                soup = BeautifulSoup(html, "html.parser")
                tag = soup.find("script", id="__NEXT_DATA__")
                if tag and tag.string:
                    data = json.loads(tag.string)
                    addrs = list(dict.fromkeys(_pluck_addresses(data)))
                    if len(addrs) >= 20:
                        log.info(f"Hypurrscan __NEXT_DATA__: {len(addrs)} addresses")
                        return addrs
                # Raw regex fallback on the entire HTML
                raw_addrs = list(dict.fromkeys(re.findall(r"0x[a-fA-F0-9]{40}", html)))
                if len(raw_addrs) >= 20:
                    log.info(f"Hypurrscan HTML regex: {len(raw_addrs)} addresses")
                    return raw_addrs
    except Exception as exc:
        log.warning(f"Hypurrscan HTML fetch failed: {exc}")

    log.warning("Hypurrscan: no usable addresses found")
    return []


async def _try_hl_leaderboard(session: aiohttp.ClientSession) -> list[str]:
    """
    Attempt B: use Hyperliquid's own info endpoint for leaderboard/ranking data.
    Multiple type values are tried; HL may or may not expose this publicly.
    """
    timeout = aiohttp.ClientTimeout(total=10)
    for type_val in ["leaderboard", "weeklyLeaderboard", "topTraders", "leaderboardRewards"]:
        try:
            async with session.post(
                HL_INFO_URL,
                json={"type": type_val},
                timeout=timeout,
            ) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    addrs = list(dict.fromkeys(_pluck_addresses(data)))
                    if len(addrs) >= 20:
                        log.info(f"HL leaderboard type={type_val!r}: {len(addrs)} addresses")
                        return addrs
        except Exception as exc:
            log.debug(f"HL type={type_val!r}: {exc}")

    log.warning("HL leaderboard endpoints: no usable addresses found")
    return []


def _load_manual_addresses() -> list[str]:
    """
    Attempt C: read addresses from scripts/manual_addresses.txt.
    Creates the file with instructions if it doesn't exist.
    """
    if not MANUAL_ADDRS_FILE.exists():
        MANUAL_ADDRS_FILE.parent.mkdir(parents=True, exist_ok=True)
        MANUAL_ADDRS_FILE.write_text(
            "# Paste one Hyperliquid trader address per line (0x…, 42 chars)\n"
            "# Find top traders at: https://app.hyperliquid.xyz/leaderboard\n"
            "# or https://hypurrscan.io\n"
            "#\n"
            "# Example:\n"
            "# 0xabcdef1234567890abcdef1234567890abcdef12\n"
        )
        log.warning(
            f"Created {MANUAL_ADDRS_FILE}. "
            "Add trader addresses there and re-run the script."
        )
        return []

    seen: set[str] = set()
    addrs = []
    for line in MANUAL_ADDRS_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and len(line) == 42 and line.startswith("0x"):
            if line not in seen:
                seen.add(line)
                addrs.append(line)
    log.info(f"Manual addresses file: {len(addrs)} unique addresses loaded")
    return addrs[:MAX_ADDRESSES]


async def collect_addresses(session: aiohttp.ClientSession) -> list[str]:
    """Full fallback chain: Hypurrscan → HL API → manual file."""
    log.info("=== Step 1: Collecting trader addresses ===")

    addrs = await _try_hypurrscan(session)
    if len(addrs) >= 50:
        return list(dict.fromkeys(addrs))[:MAX_ADDRESSES]

    addrs = await _try_hl_leaderboard(session)
    if len(addrs) >= 50:
        return list(dict.fromkeys(addrs))[:MAX_ADDRESSES]

    addrs = _load_manual_addresses()
    if not addrs:
        log.error(
            "No addresses available from any source. "
            f"Add addresses to {MANUAL_ADDRS_FILE} and re-run."
        )
        sys.exit(1)

    return addrs


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Fetch positions
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_one_state(
    session: aiohttp.ClientSession,
    address: str,
    sem: asyncio.Semaphore,
    idx: int,
    total: int,
) -> Optional[dict]:
    async with sem:
        try:
            async with session.post(
                HL_INFO_URL,
                json={"type": "clearinghouseState", "user": address},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 429:
                    log.warning(f"[{idx}/{total}] 429 rate-limited – backing off 8s")
                    await asyncio.sleep(8)
                    return None
                if r.status != 200:
                    log.debug(f"[{idx}/{total}] HTTP {r.status} for {address[:10]}…")
                    return None
                return await r.json(content_type=None)
        except Exception as exc:
            log.debug(f"[{idx}/{total}] {address[:10]}… error: {exc}")
            return None
        finally:
            await asyncio.sleep(SLEEP_BETWEEN_CALLS)


BATCH_SIZE       = 250    # addresses per batch
BATCH_SLEEP_S    = 30.0   # sleep between batches (rate budget: 250*2=500 weight/batch)
# Rate math: 500 weight / ~35s (batch_time + sleep) ≈ 857 weight/min  < 1200 limit ✓

async def fetch_all_positions(
    session: aiohttp.ClientSession, addresses: list[str]
) -> list[dict]:
    total      = len(addresses)
    n_batches  = (total + BATCH_SIZE - 1) // BATCH_SIZE
    eta_s      = n_batches * BATCH_SLEEP_S
    log.info(f"=== Step 2: Fetching clearinghouseState for {total} addresses ===")
    log.info(
        f"    Batches: {n_batches} × {BATCH_SIZE}  |  "
        f"Semaphore={SEMAPHORE_LIMIT}  |  "
        f"Inter-batch sleep={BATCH_SLEEP_S}s  |  "
        f"ETA ≈{eta_s/60:.1f}min"
    )

    sem    = asyncio.Semaphore(SEMAPHORE_LIMIT)
    states: list[dict] = []

    for b_idx, batch_start in enumerate(range(0, total, BATCH_SIZE)):
        batch   = addresses[batch_start : batch_start + BATCH_SIZE]
        abs_idx = batch_start  # offset for per-request logging
        tasks   = [
            _fetch_one_state(session, addr, sem, abs_idx + i + 1, total)
            for i, addr in enumerate(batch)
        ]
        results = await asyncio.gather(*tasks)
        ok      = [r for r in results if r is not None]
        states.extend(ok)
        log.info(
            f"    Batch {b_idx+1}/{n_batches} done: "
            f"{len(ok)}/{len(batch)} OK  |  "
            f"cumulative {len(states)}/{batch_start + len(batch)}"
        )
        if b_idx + 1 < n_batches:
            log.info(f"    Sleeping {BATCH_SLEEP_S}s before next batch…")
            await asyncio.sleep(BATCH_SLEEP_S)

    log.info(f"    Total received: {len(states)}/{total}")
    return states


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Build liquidation density map
# ─────────────────────────────────────────────────────────────────────────────

def _make_bins(price: float) -> list[tuple[float, float]]:
    """
    Return a list of (low, high) price bins covering [price*(1-5%), price*(1+5%)].
    Near zone (±2%): 0.1% bins; far zone: 0.5% bins.
    """
    low    = price * (1 - PRICE_RANGE_PCT)
    high   = price * (1 + PRICE_RANGE_PCT)
    n_low  = price * (1 - FAR_THRESHOLD_PCT)
    n_high = price * (1 + FAR_THRESHOLD_PCT)

    bins: list[tuple[float, float]] = []

    def _add_zone(start: float, end: float, step_pct: float) -> None:
        p = start
        step = price * step_pct
        while p < end - 1e-9:
            top = min(p + step, end)
            bins.append((p, top))
            p = top

    _add_zone(low, n_low, FAR_BIN_PCT)
    _add_zone(n_low, n_high, NEAR_BIN_PCT)
    _add_zone(n_high, high, FAR_BIN_PCT)

    return bins


def build_liq_density(
    states: list[dict],
    symbol: str,
    current_price: float,
) -> tuple[pd.DataFrame, dict]:
    """
    Bin liquidation prices from all fetched positions into a density DataFrame.

    Returns (df, diag) where diag holds step-by-step funnel counts.
    DataFrame columns: bin_low, bin_high, bin_mid, long_usd, short_usd, total_usd
    """
    bins = _make_bins(current_price)
    long_usd  = np.zeros(len(bins))
    short_usd = np.zeros(len(bins))

    price_lo = current_price * (1 - PRICE_RANGE_PCT)
    price_hi = current_price * (1 + PRICE_RANGE_PCT)

    diag = {
        "addrs_queried":       len(states),
        "addrs_with_any_pos":  0,
        "pos_this_symbol":     0,
        "pos_has_liq_px":      0,
        "pos_liq_in_range":    0,
    }
    addr_had_pos: set[int] = set()

    for idx, state in enumerate(states):
        if not isinstance(state, dict):
            continue
        asset_positions = state.get("assetPositions", [])
        if asset_positions:
            diag["addrs_with_any_pos"] += 1

        for ap in asset_positions:
            pos = ap.get("position", {}) if isinstance(ap, dict) else {}
            if pos.get("coin") != symbol:
                continue

            diag["pos_this_symbol"] += 1

            raw_liq = pos.get("liquidationPx")
            if raw_liq is None:
                continue
            try:
                liq_px = float(raw_liq)
            except (ValueError, TypeError):
                continue
            if liq_px <= 0:
                continue

            diag["pos_has_liq_px"] += 1

            if not (price_lo <= liq_px <= price_hi):
                continue

            try:
                szi = float(pos.get("szi") or 0)
                pos_val = abs(float(pos.get("positionValue") or 0))
            except (ValueError, TypeError):
                continue
            if pos_val == 0:
                continue

            diag["pos_liq_in_range"] += 1
            is_long = szi > 0

            for i, (b_lo, b_hi) in enumerate(bins):
                if b_lo <= liq_px < b_hi:
                    if is_long:
                        long_usd[i]  += pos_val
                    else:
                        short_usd[i] += pos_val
                    break

    log.info(
        f"  {symbol}: {diag['pos_this_symbol']} positions, "
        f"{diag['pos_has_liq_px']} have liqPx, "
        f"{diag['pos_liq_in_range']} inside ±5%"
    )

    df = pd.DataFrame({
        "bin_low":   [b[0] for b in bins],
        "bin_high":  [b[1] for b in bins],
        "bin_mid":   [(b[0] + b[1]) / 2 for b in bins],
        "long_usd":  long_usd,
        "short_usd": short_usd,
    })
    df["total_usd"] = df["long_usd"] + df["short_usd"]
    return df, diag


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Fetch candles
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    hours: int = 24,
) -> pd.DataFrame:
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - hours * 3600 * 1000
    payload  = {
        "type": "candleSnapshot",
        "req": {
            "coin":      symbol,
            "interval":  "5m",
            "startTime": start_ms,
            "endTime":   now_ms,
        },
    }
    try:
        async with session.post(
            HL_INFO_URL, json=payload, timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            if r.status != 200:
                log.warning(f"candleSnapshot {symbol}: HTTP {r.status}")
                return pd.DataFrame()
            raw = await r.json(content_type=None)

        if not raw:
            log.warning(f"candleSnapshot {symbol}: empty response")
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        # HL candle fields: t=open_ms, T=close_ms, o, h, l, c, v, n
        rename_map = {
            "t": "open_time", "T": "close_time",
            "o": "open", "h": "high", "l": "low",
            "c": "close", "v": "volume", "n": "trades",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "open_time" in df.columns:
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

        log.info(f"  Candles {symbol}: {len(df)} bars fetched")
        return df

    except Exception as exc:
        log.warning(f"fetch_candles {symbol}: {exc}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Plot
# ─────────────────────────────────────────────────────────────────────────────

def _draw_candles(ax: plt.Axes, df: pd.DataFrame) -> None:
    """Draw OHLC bars manually on ax; x-axis = integer index."""
    for i, row in enumerate(df.itertuples(index=False)):
        try:
            o, h, l, c = float(row.open), float(row.high), float(row.low), float(row.close)
        except Exception:
            continue
        color = "#26a69a" if c >= o else "#ef5350"
        body_lo = min(o, c)
        body_h  = max(abs(c - o), (h - l) * 0.005)   # never zero height
        ax.add_patch(
            mpatches.Rectangle(
                (i - 0.35, body_lo), 0.7, body_h,
                color=color, zorder=2, linewidth=0,
            )
        )
        ax.plot([i, i], [l, h], color=color, linewidth=0.7, zorder=1)

    ax.set_xlim(-1, len(df))
    valid_l = df["low"].dropna()
    valid_h = df["high"].dropna()
    if not valid_l.empty and not valid_h.empty:
        margin = (valid_h.max() - valid_l.min()) * 0.01
        ax.set_ylim(valid_l.min() - margin, valid_h.max() + margin)

    # x-axis time labels every ~2 hours (24 bars of 5m each)
    step = max(1, len(df) // 12)
    ticks = list(range(0, len(df), step))
    labels = []
    for t in ticks:
        try:
            labels.append(df["open_time"].iloc[t].strftime("%H:%M"))
        except Exception:
            labels.append("")
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels, fontsize=7)


def plot_symbol(
    symbol:        str,
    density:       pd.DataFrame,
    candles:       pd.DataFrame,
    current_price: float,
    timestamp:     str,
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    BG   = "#0d0d1a"
    FACE = "#1a1a2e"

    fig = plt.figure(figsize=(20, 11), facecolor=BG)
    fig.suptitle(
        f"{symbol} — Liquidation Density vs 24h Price Action   "
        f"(Current: ${current_price:,.2f})   {timestamp} UTC",
        color="white", fontsize=13, fontweight="bold",
    )

    gs      = fig.add_gridspec(1, 2, wspace=0.05, left=0.07, right=0.97, top=0.93, bottom=0.07)
    ax_liq  = fig.add_subplot(gs[0, 0])
    ax_ohlc = fig.add_subplot(gs[0, 1])

    # ── Cluster levels used on both panels ───────────────────────────────────
    clusters = density[density["total_usd"] >= CLUSTER_USD].copy()

    # ── LEFT: horizontal bar chart ───────────────────────────────────────────
    y         = density["bin_mid"].values
    bar_h     = (density["bin_high"] - density["bin_low"]).values * 0.88
    long_m    = density["long_usd"].values  / 1e6
    short_m   = density["short_usd"].values / 1e6

    ax_liq.barh(y, -long_m,  height=bar_h, color="#26a69a", alpha=0.85, label="Long liq (left)")
    ax_liq.barh(y,  short_m, height=bar_h, color="#ef5350", alpha=0.85, label="Short liq (right)")

    ax_liq.axhline(current_price, color="white",  linestyle="--", linewidth=1.4, zorder=5,
                   label=f"Price ${current_price:,.0f}")
    for _, row in clusters.iterrows():
        ax_liq.axhline(
            row["bin_mid"], color="yellow", linestyle=":", linewidth=0.9, alpha=0.7, zorder=4
        )

    ax_liq.set_facecolor(FACE)
    ax_liq.set_title("Liquidation Density (±5%)", color="white", fontsize=11)
    ax_liq.set_xlabel("Liquidation Size ($M)  ← longs | shorts →", color="#aaa", fontsize=9)
    ax_liq.set_ylabel("Price", color="#aaa", fontsize=9)
    ax_liq.tick_params(colors="white")
    for spine in ax_liq.spines.values():
        spine.set_color("#444")
    ax_liq.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Centre the x-axis on 0, symmetric
    max_bar = max(long_m.max(), short_m.max(), 1.0)
    ax_liq.set_xlim(-max_bar * 1.15, max_bar * 1.15)
    ax_liq.axvline(0, color="#555", linewidth=0.5)
    ax_liq.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${abs(x):.0f}M"))
    ax_liq.legend(loc="upper right", fontsize=8, labelcolor="white",
                  facecolor="#111", edgecolor="#555")

    # ── RIGHT: OHLC candlestick ───────────────────────────────────────────────
    ax_ohlc.set_facecolor(FACE)
    ax_ohlc.set_title("24h Price Action (5m candles)", color="white", fontsize=11)
    ax_ohlc.tick_params(colors="white", labelleft=False, labelright=True)
    ax_ohlc.yaxis.set_label_position("right")
    ax_ohlc.yaxis.tick_right()
    ax_ohlc.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    for spine in ax_ohlc.spines.values():
        spine.set_color("#444")

    if not candles.empty:
        _draw_candles(ax_ohlc, candles)
        ax_ohlc.axhline(current_price, color="white", linestyle="--",
                        linewidth=1.4, zorder=5, label=f"${current_price:,.0f}")
        for _, row in clusters.iterrows():
            ax_ohlc.axhline(
                row["bin_mid"], color="yellow", linestyle=":", linewidth=0.9, alpha=0.7,
                label=f"Cluster ${row['bin_mid']:,.0f}",
            )
        handles, labels = ax_ohlc.get_legend_handles_labels()
        # Deduplicate legend entries
        seen: dict[str, object] = {}
        for h, l in zip(handles, labels):
            seen.setdefault(l, h)
        ax_ohlc.legend(
            seen.values(), seen.keys(),
            loc="upper right", fontsize=8, labelcolor="white",
            facecolor="#111", edgecolor="#555",
        )
    else:
        ax_ohlc.text(
            0.5, 0.5, "Candle data unavailable",
            transform=ax_ohlc.transAxes,
            ha="center", va="center", color="white", fontsize=12,
        )

    out_path = OUTPUT_DIR / f"feasibility_{symbol}_{timestamp}.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"  Chart saved → {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Text report
# ─────────────────────────────────────────────────────────────────────────────

def print_report(
    symbol:        str,
    density:       pd.DataFrame,
    current_price: float,
    diag:          dict,
) -> None:
    total_long  = density["long_usd"].sum()
    total_short = density["short_usd"].sum()
    total_all   = total_long + total_short

    print(f"\n{'═'*64}")
    print(f"  {symbol}  LIQUIDATION DENSITY SNAPSHOT")
    print(f"{'═'*64}")
    print(f"  Current price   : ${current_price:>12,.2f}")
    print(f"  Total long  liq : ${total_long  / 1e6:>10.2f}M")
    print(f"  Total short liq : ${total_short / 1e6:>10.2f}M")
    print(f"  Total (in range): ${total_all   / 1e6:>10.2f}M")
    if total_all > 0:
        ls_ratio = total_long / total_all * 100
        print(f"  Long/Short bias : {ls_ratio:.1f}% long / {100-ls_ratio:.1f}% short")

    print(f"\n  ── Diagnostic funnel ──────────────────────────────────")
    print(f"  Addresses queried              : {diag['addrs_queried']:>6}")
    print(f"  Addresses with any position    : {diag['addrs_with_any_pos']:>6}")
    print(f"  {symbol} positions found             : {diag['pos_this_symbol']:>6}")
    print(f"  {symbol} positions with liquidationPx: {diag['pos_has_liq_px']:>6}")
    print(f"  {symbol} liqPx inside ±5% range      : {diag['pos_liq_in_range']:>6}")
    print(f"  ────────────────────────────────────────────────────────")

    print(f"\n  Top 5 zones by total liquidation USD:")
    top5 = density.nlargest(5, "total_usd")
    print(f"  {'Price':>10}  {'Dist%':>7}  {'Long $M':>9}  {'Short $M':>9}  {'Total $M':>9}")
    print(f"  {'─'*10}  {'─'*7}  {'─'*9}  {'─'*9}  {'─'*9}")
    for _, row in top5.iterrows():
        dist_pct = (row["bin_mid"] - current_price) / current_price * 100
        side     = "▲" if dist_pct > 0 else "▼"
        print(
            f"  ${row['bin_mid']:>9,.0f}  "
            f"{side}{abs(dist_pct):5.2f}%  "
            f"${row['long_usd']/1e6:>8.2f}  "
            f"${row['short_usd']/1e6:>8.2f}  "
            f"${row['total_usd']/1e6:>8.2f}"
        )

    clusters = density[density["total_usd"] >= CLUSTER_USD].copy()
    if not clusters.empty:
        clusters = clusters.copy()
        clusters["dist_abs"] = (clusters["bin_mid"] - current_price).abs()
        nearest = clusters.nsmallest(1, "dist_abs").iloc[0]
        dist_pct = (nearest["bin_mid"] - current_price) / current_price * 100
        side     = "above" if dist_pct > 0 else "below"
        print(
            f"\n  Nearest cluster (>${CLUSTER_USD/1e6:.0f}M):  "
            f"${nearest['bin_mid']:,.0f}  "
            f"({abs(dist_pct):.2f}% {side})  "
            f"total ${nearest['total_usd']/1e6:.2f}M"
        )
    else:
        print(f"\n  No clusters above ${CLUSTER_USD/1e6:.0f}M found in sampled addresses.")
        print(f"  This may mean: (a) sample too small, (b) address source failed,")
        print(f"  or (c) genuinely sparse liq distribution.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    headers   = {"Content-Type": "application/json"}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:

        # 1. Addresses
        addresses = await collect_addresses(session)
        log.info(f"Proceeding with {len(addresses)} addresses")

        # 2. Current prices
        log.info("=== Fetching current mid prices ===")
        try:
            async with session.post(HL_INFO_URL, json={"type": "allMids"}) as r:
                mids: dict[str, float] = {k: float(v) for k, v in (await r.json(content_type=None)).items()}
        except Exception as exc:
            log.error(f"allMids failed: {exc}")
            sys.exit(1)

        for sym in SYMBOLS:
            if sym not in mids:
                log.error(f"{sym} not in allMids — check symbol name")
                sys.exit(1)
        log.info("  " + "  |  ".join(f"{s}: ${mids[s]:,.2f}" for s in SYMBOLS))

        # 3. Positions
        states = await fetch_all_positions(session, addresses)
        if not states:
            log.error("Zero positions fetched — can't build density map. Aborting.")
            sys.exit(1)

        # 4–6. Per-symbol processing
        log.info("=== Step 3-6: Building density maps and charts ===")
        for symbol in SYMBOLS:
            current_price = mids[symbol]
            log.info(f"--- {symbol} @ ${current_price:,.2f} ---")

            density, diag = build_liq_density(states, symbol, current_price)
            candles = await fetch_candles(session, symbol)
            print_report(symbol, density, current_price, diag)
            plot_symbol(symbol, density, candles, current_price, ts)

    print(f"\n{'═'*64}")
    print("  All charts saved to: output/")
    print(f"  Timestamp: {ts}")
    print()
    print("  ┌─ DECISION ─────────────────────────────────────────────┐")
    print("  │  Open the PNG files and ask:                           │")
    print("  │  Do yellow cluster lines align with price reactions?   │")
    print("  │  YES → proceed to Phase 1 (historical backtest)        │")
    print("  │  NO  → stop project, hypothesis likely invalid         │")
    print("  └────────────────────────────────────────────────────────┘")
    print()


if __name__ == "__main__":
    asyncio.run(main())
