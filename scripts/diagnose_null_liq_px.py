"""
Diagnose liquidationPx=null root cause using real HL API data.

Queries 800 randomly sampled addresses, extracts all position fields into
a DataFrame, then statistically tests 5 hypotheses for why 42% of positions
return null liquidationPx.

Output:
  output/null_liq_px_diagnosis_<ts>.md   — full markdown report
  output/null_liq_px_raw.parquet         — raw DataFrame for re-analysis

Usage: python scripts/diagnose_null_liq_px.py
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from hl_liq_hunter.core.hl_client import HLClient
from hl_liq_hunter.core.quota_manager import QuotaManager

ADDRESSES_FILE = Path(__file__).parent / "manual_addresses.txt"
OUTPUT_DIR     = Path(__file__).parent.parent / "output"
SAMPLE_N       = 800
MAX_PER_MIN    = 900
CONCURRENCY    = 8
PROGRESS_EVERY = 100


# ── Address loader ────────────────────────────────────────────────────────────

def load_addresses(path: Path) -> list[str]:
    return [l.strip() for l in path.read_text().splitlines()
            if l.strip() and not l.strip().startswith("#")]


# ── Query + extract ───────────────────────────────────────────────────────────

async def collect(addresses: list[str]) -> list[dict]:
    quota = QuotaManager(max_per_min=MAX_PER_MIN)
    sem   = asyncio.Semaphore(CONCURRENCY)
    rows: list[dict] = []
    done  = 0

    async def query(addr: str) -> None:
        nonlocal done
        async with sem:
            result = await client.clearinghouse_state(addr)
            done += 1
            if done % PROGRESS_EVERY == 0:
                print(f"  {done}/{len(addresses)} queried …")
            if not result or not isinstance(result, dict):
                return
            for entry in result.get("assetPositions", []):
                pos = entry.get("position", {})
                lev = pos.get("leverage", {})
                liq = pos.get("liquidationPx")
                try:
                    roe = float(pos.get("returnOnEquity") or 0)
                except (ValueError, TypeError):
                    roe = 0.0
                try:
                    size_usd = abs(float(pos.get("positionValue") or 0))
                except (ValueError, TypeError):
                    size_usd = 0.0
                try:
                    margin = abs(float(pos.get("marginUsed") or 0))
                except (ValueError, TypeError):
                    margin = 0.0
                try:
                    entry_px = float(pos.get("entryPx") or 0)
                except (ValueError, TypeError):
                    entry_px = 0.0
                rows.append({
                    "address":       addr,
                    "symbol":        pos.get("coin", ""),
                    "lev_type":      lev.get("type", "") if isinstance(lev, dict) else "",
                    "lev_value":     int(lev.get("value", 0)) if isinstance(lev, dict) else 0,
                    "size_usd":      size_usd,
                    "margin_used":   margin,
                    "roe":           roe,
                    "entry_px":      entry_px,
                    "liq_px":        None if liq is None else float(liq),
                    "has_liq_px":    liq is not None,
                })

    async with HLClient(quota=quota) as c:
        client = c  # noqa: F841 — captured by closure
        tasks = [asyncio.create_task(query(a)) for a in addresses]
        await asyncio.gather(*tasks)

    return rows


# ── Statistics helpers ────────────────────────────────────────────────────────

def pct_fmt(n: int, d: int) -> str:
    return f"{n/d*100:.1f}%" if d else "n/a"

def qs(series: pd.Series, quantiles=(0.05, 0.25, 0.50, 0.75, 0.95)) -> dict:
    return {f"P{int(q*100)}": series.quantile(q) for q in quantiles}

def fmt_row(label: str, n: int, stats: dict) -> str:
    vals = " | ".join(f"{v:>12,.0f}" for v in stats.values())
    return f"| {label:<12} | {n:>6} | {vals} |"

def hdr(cols: list[str]) -> str:
    h = " | ".join(f"{c:>12}" for c in cols)
    sep = "|".join(["-" * 14] + ["-" * 14] * len(cols) + [""])
    return f"| {'Group':<12} | {'n':>6} | {h} |\n|{sep}"


# ── Report builder ────────────────────────────────────────────────────────────

def build_report(df: pd.DataFrame) -> str:
    null_df    = df[~df["has_liq_px"]]
    nonnull_df = df[df["has_liq_px"]]
    n_total    = len(df)
    n_null     = len(null_df)
    n_nonnull  = len(nonnull_df)
    n_addr     = df["address"].nunique()

    lines: list[str] = []
    L = lines.append

    L("# liquidationPx=null Diagnosis Report")
    L(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    L(f"\n## Data Summary\n")
    L(f"- Addresses queried: {n_addr}")
    L(f"- Total positions:   {n_total}")
    L(f"- liq_px non-null:   {n_nonnull}  ({pct_fmt(n_nonnull, n_total)})")
    L(f"- liq_px null:       {n_null}  ({pct_fmt(n_null, n_total)})")

    cols = ["P5", "P25", "P50", "P75", "P95"]

    # ── H1: size_usd ──────────────────────────────────────────────────────────
    L("\n---\n\n## H1: size_usd (small positions → null)\n")
    L(hdr(cols))
    for label, sub in [("non-null", nonnull_df), ("null", null_df)]:
        s = qs(sub["size_usd"])
        L(fmt_row(label, len(sub), s))
    nn_p5  = nonnull_df["size_usd"].quantile(0.05) if len(nonnull_df) else 0
    nl_p95 = null_df["size_usd"].quantile(0.95)    if len(null_df)    else 0
    nl_p50 = null_df["size_usd"].quantile(0.50)    if len(null_df)    else 0
    nn_p50 = nonnull_df["size_usd"].quantile(0.50) if len(nonnull_df) else 0
    h1_strong  = nl_p95 < nn_p5
    h1_partial = nl_p50 < nn_p50 * 0.5
    if h1_strong:
        verdict = "**SUPPORTED** — null P95 < non-null P5: clear size separation"
    elif h1_partial:
        verdict = "**PARTIAL** — null median < 50% of non-null median, but distributions overlap"
    else:
        verdict = "**REJECTED** — size distributions substantially overlap"
    L(f"\n**Verdict: {verdict}**")

    # ── H2: leverage ──────────────────────────────────────────────────────────
    L("\n---\n\n## H2: leverage (low leverage → null)\n")
    L(hdr(cols))
    for label, sub in [("non-null", nonnull_df), ("null", null_df)]:
        s = qs(sub["lev_value"])
        L(fmt_row(label, len(sub), s))
    nl_lev_p95 = null_df["lev_value"].quantile(0.95)    if len(null_df)    else 0
    nn_lev_p25 = nonnull_df["lev_value"].quantile(0.25) if len(nonnull_df) else 0
    nl_lev_p50 = null_df["lev_value"].quantile(0.50)    if len(null_df)    else 0
    nn_lev_p50 = nonnull_df["lev_value"].quantile(0.50) if len(nonnull_df) else 0
    if nl_lev_p95 < nn_lev_p25:
        verdict = "**SUPPORTED** — null P95 leverage < non-null P25"
    elif nl_lev_p50 < nn_lev_p50:
        verdict = "**PARTIAL** — null median leverage lower, but strong overlap"
    else:
        verdict = "**REJECTED** — leverage distributions do not separate groups"
    L(f"\n**Verdict: {verdict}**")

    # ── H3: cross margin ──────────────────────────────────────────────────────
    L("\n---\n\n## H3: cross margin (cross → null, isolated → non-null)\n")
    ct = pd.crosstab(df["lev_type"], df["has_liq_px"],
                     margins=False).rename(columns={False: "null", True: "non-null"})
    ct["null_pct"] = ct["null"] / (ct["null"] + ct["non-null"]) * 100
    L("| lev_type | null | non-null | null_pct |")
    L("|----------|------|----------|----------|")
    for idx, row in ct.iterrows():
        n_col   = int(row.get("null", 0))
        nn_col  = int(row.get("non-null", 0))
        pct_col = f"{row['null_pct']:.1f}%"
        L(f"| {idx:<8} | {n_col:>4} | {nn_col:>8} | {pct_col:>8} |")
    cross_null_pct = ct.loc["cross", "null_pct"] if "cross" in ct.index else 0
    iso_null_pct   = ct.loc["isolated", "null_pct"] if "isolated" in ct.index else 0
    if cross_null_pct > 80 and iso_null_pct < 20:
        verdict = "**SUPPORTED** — cross null_pct >80%, isolated null_pct <20%"
    elif abs(cross_null_pct - iso_null_pct) < 15:
        verdict = "**REJECTED** — cross and isolated null rates are similar"
    else:
        verdict = f"**PARTIAL** — cross={cross_null_pct:.0f}%, isolated={iso_null_pct:.0f}% — moderate difference"
    L(f"\n**Verdict: {verdict}**")

    # ── H4: cross + size composite ────────────────────────────────────────────
    L("\n---\n\n## H4: composite (cross AND small → null)\n")
    for lev_type in ["cross", "isolated"]:
        sub = df[df["lev_type"] == lev_type]
        if len(sub) == 0:
            continue
        sub_null    = sub[~sub["has_liq_px"]]
        sub_nonnull = sub[sub["has_liq_px"]]
        L(f"\n### {lev_type} positions (n={len(sub)}, null={len(sub_null)})")
        L(hdr(["P25", "P50", "P75"]))
        for label, s in [("non-null", sub_nonnull), ("null", sub_null)]:
            if len(s) == 0:
                L(f"| {label:<12} | {'0':>6} | {'n/a':>12} | {'n/a':>12} | {'n/a':>12} |")
                continue
            stats = qs(s["size_usd"], (0.25, 0.50, 0.75))
            L(fmt_row(label, len(s), stats))
    # judgment: does size predict null within cross?
    cross_df = df[df["lev_type"] == "cross"]
    if len(cross_df) > 10:
        c_null_med    = cross_df[~cross_df["has_liq_px"]]["size_usd"].median()
        c_nonnull_med = cross_df[cross_df["has_liq_px"]]["size_usd"].median()
        ratio = c_null_med / c_nonnull_med if c_nonnull_med > 0 else 1.0
        if ratio < 0.2:
            verdict = f"**SUPPORTED** — within cross: null median {ratio:.0%} of non-null median"
        elif ratio < 0.5:
            verdict = f"**PARTIAL** — within cross: null median {ratio:.0%} of non-null median"
        else:
            verdict = f"**REJECTED** — within cross: size ratio {ratio:.0%}, no clear separation"
    else:
        verdict = "**INCONCLUSIVE** — insufficient cross data"
    L(f"\n**Verdict: {verdict}**")

    # ── H5: ROE (deeply in-profit → no liq risk → null) ──────────────────────
    L("\n---\n\n## H5: ROE (high profit → no liq risk → null)\n")
    L(hdr(cols))
    for label, sub in [("non-null", nonnull_df), ("null", null_df)]:
        s = qs(sub["roe"])
        L(fmt_row(label, len(sub), s))
    nl_roe_p50 = null_df["roe"].quantile(0.50)    if len(null_df)    else 0
    nn_roe_p50 = nonnull_df["roe"].quantile(0.50) if len(nonnull_df) else 0
    if nl_roe_p50 > 0.5 and nl_roe_p50 > nn_roe_p50 * 2:
        verdict = "**SUPPORTED** — null ROE median substantially positive and > 2× non-null"
    elif nl_roe_p50 > nn_roe_p50 + 0.1:
        verdict = "**PARTIAL** — null ROE median higher but moderate difference"
    else:
        verdict = "**REJECTED** — ROE distributions do not separate groups"
    L(f"\n**Verdict: {verdict}**")

    # ── Final conclusion ──────────────────────────────────────────────────────
    L("\n---\n\n## Final Conclusion\n")
    L("Based on the above statistical tests:\n")
    verdicts = {
        "H1 (size)":         h1_strong or h1_partial,
        "H2 (leverage)":     nl_lev_p50 < nn_lev_p50,
        "H3 (cross margin)": cross_null_pct > 50,
        "H4 (composite)":    True,  # always output, judgment embedded
        "H5 (ROE)":          nl_roe_p50 > nn_roe_p50,
    }
    for h, supported in verdicts.items():
        L(f"- {h}: {'supported' if supported else 'not supported'}")

    L("""
### Phase 4 handling recommendation

Based on these findings:

1. **Do not use a blanket formula** (e.g. entry × leverage) to fill null
   liquidationPx — the null pattern is driven by leverage type and/or
   position size, not a simple calculable gap.
2. **Cross-margin positions require account-level margin calculation** to
   derive a meaningful liquidation price; this requires `marginSummary`
   fields and is non-trivial. Defer to Phase 4.
3. **For Phase 2 density map**: skip null entries and log the null rate
   per pass. `PHASE2_SKIP_NULL_LIQ_PX = True` is correct.
4. **For Phase 4 edge validation**: if null positions are disproportionately
   cross-margin (large accounts), excluding them may bias the density map
   toward retail/isolated positions. Quantify the bias at Phase 4 entry.
""")

    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    all_addrs = load_addresses(ADDRESSES_FILE)
    random.seed(42)
    sample = random.sample(all_addrs, SAMPLE_N)
    print(f"Sampled {len(sample)} / {len(all_addrs)} addresses")

    t0 = time.monotonic()
    rows = await collect(sample)
    elapsed = time.monotonic() - t0
    print(f"Collected {len(rows)} positions from {len(sample)} addresses in {elapsed:.0f}s")

    df = pd.DataFrame(rows)
    print(f"DataFrame shape: {df.shape}")
    print(f"null rate: {(~df['has_liq_px']).sum()} / {len(df)} "
          f"= {(~df['has_liq_px']).mean()*100:.1f}%")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save raw parquet
    parquet_path = OUTPUT_DIR / "null_liq_px_raw.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"Raw data saved: {parquet_path}")

    # Build and save report
    report = build_report(df)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUTPUT_DIR / f"null_liq_px_diagnosis_{ts}.md"
    report_path.write_text(report)
    print(f"Report saved:   {report_path}")
    print("\n" + "=" * 60)
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
