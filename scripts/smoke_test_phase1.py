"""
Phase 1 smoke test — validates QuotaManager, AddressRegistry, HLClient
against the real HL API.

Run:   python scripts/smoke_test_phase1.py
Stops: after 600 seconds (hard deadline) or when Ctrl-C.
Output: diagnostic report to stdout + raw JSON samples in output/.

This script is DISPOSABLE — delete after Phase 2 kick-off.
Do NOT modify Phase 1 source code if this script reveals issues;
record them in the report and wait for decision.
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import time
from collections import Counter
from pathlib import Path

from hl_liq_hunter.config import Tier
from hl_liq_hunter.core.address_registry import AddressRegistry
from hl_liq_hunter.core.hl_client import HLClient
from hl_liq_hunter.core.quota_manager import QuotaManager

# ── Constants ─────────────────────────────────────────────────────────────────

ADDRESSES_FILE = Path(__file__).parent / "manual_addresses.txt"
OUTPUT_DIR     = Path(__file__).parent.parent / "output"
DURATION_S     = 600          # hard deadline
CONCURRENCY    = 15           # asyncio.Semaphore limit
STATUS_EVERY_S = 60           # periodic status interval
PROGRESS_EVERY = 100          # print progress every N completed queries
RAW_SAMPLE_CAP = 5            # save this many raw responses with positions

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("smoke_test")


class _RateLimitCounter(logging.Handler):
    """Intercepts hl_client WARNING logs to count server-side 429s."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if "rate limited" in record.getMessage():
            self.count += 1


_rl_counter = _RateLimitCounter()
logging.getLogger("hl_liq_hunter.core.hl_client").addHandler(_rl_counter)


# ── Address loader ─────────────────────────────────────────────────────────────

def load_addresses(path: Path) -> list[str]:
    addrs: list[str] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            addrs.append(line)
    return addrs


# ── Per-call result ───────────────────────────────────────────────────────────

# failure_reason values: "none_other" | "429_sleep" (detected via log counter delta)
# We track raw None returns and separately count 429s via the log handler.

# ── Main scanner ──────────────────────────────────────────────────────────────

async def run_smoke(addresses: list[str]) -> None:
    deadline = time.monotonic() + DURATION_S

    quota    = QuotaManager(max_per_min=1000)
    registry = AddressRegistry()

    log.info("Registering %d addresses …", len(addresses))
    for addr in addresses:
        await registry.add_address(addr)
    log.info("Registry initialised: %s", registry.stats())

    # ── Per-run accumulators ───────────────────────────────────────────────────
    latencies_ms: list[float] = []
    success_count   = 0
    failure_count   = 0
    scanned_addrs: set[str] = set()

    # position-field stats
    ap_len_dist: Counter[str] = Counter()      # "0"/"1"/"2"/"3+"
    liq_px_null = 0
    liq_px_total = 0
    leverage_types: Counter[str] = Counter()
    position_usd_values: list[float] = []

    # raw samples: list[tuple[str, dict]]  (address, raw_resp)
    raw_samples: list[tuple[str, dict]] = []  # type: ignore[type-arg]

    # passes over the address list
    passes_done = 0
    queries_completed = 0
    peak_utilization  = 0.0

    last_status_t   = time.monotonic()
    rl_count_at_start = _rl_counter.count

    sem = asyncio.Semaphore(CONCURRENCY)

    async def query_one(addr: str) -> None:
        nonlocal success_count, failure_count, queries_completed, peak_utilization
        nonlocal liq_px_null, liq_px_total

        async with sem:
            if time.monotonic() >= deadline:
                return

            t0 = time.monotonic()
            result = await client.clearinghouse_state(addr)
            elapsed_ms = (time.monotonic() - t0) * 1000
            latencies_ms.append(elapsed_ms)

            scanned_addrs.add(addr)

            if result is None:
                failure_count += 1
            else:
                success_count += 1

                # ── Tier update ──────────────────────────────────────────────
                positions = result.get("assetPositions", [])
                total_usd = 0.0
                has_pos   = False

                ap_len = len(positions)
                if ap_len == 0:
                    ap_len_dist["0"] += 1
                elif ap_len == 1:
                    ap_len_dist["1"] += 1
                elif ap_len == 2:
                    ap_len_dist["2"] += 1
                else:
                    ap_len_dist["3+"] += 1

                for pos_entry in positions:
                    pos = pos_entry.get("position", {})
                    # leverage type
                    lev = pos.get("leverage", {})
                    lev_type = lev.get("type", "unknown") if isinstance(lev, dict) else "unknown"
                    leverage_types[str(lev_type)] += 1

                    # liquidationPx
                    liq_px = pos.get("liquidationPx")
                    liq_px_total += 1
                    if liq_px is None:
                        liq_px_null += 1

                    # position value
                    try:
                        pos_val = abs(float(pos.get("positionValue", 0) or 0))
                        if pos_val > 0:
                            has_pos = True
                            total_usd += pos_val
                    except (ValueError, TypeError):
                        pass

                if has_pos:
                    position_usd_values.append(total_usd)

                await registry.update_after_scan(
                    addr,
                    total_usd=total_usd,
                    has_position=has_pos,
                    scanned_at=time.time(),
                )

                # raw sample capture
                if has_pos and len(raw_samples) < RAW_SAMPLE_CAP:
                    raw_samples.append((addr, result))

            # quota peak
            util = quota.stats()["utilization"]
            if isinstance(util, float) and util > peak_utilization:
                peak_utilization = util

            queries_completed += 1
            if queries_completed % PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - (deadline - DURATION_S)
                log.info(
                    "[progress] %d queries | %.0fs elapsed | "
                    "quota %.0f%% | ok=%d fail=%d",
                    queries_completed,
                    elapsed,
                    util * 100 if isinstance(util, float) else 0,
                    success_count,
                    failure_count,
                )

    # ── Status printer ─────────────────────────────────────────────────────────
    async def status_printer() -> None:
        nonlocal last_status_t
        while time.monotonic() < deadline:
            await asyncio.sleep(STATUS_EVERY_S)
            s = registry.stats()
            q = quota.stats()
            log.info(
                "[status] registry=%s | quota_usage=%d/%d (%.0f%%)",
                s, q["usage_per_min"], q["max_per_min"],
                (q["utilization"] if isinstance(q["utilization"], float) else 0) * 100,
            )

    # ── Scanner passes ─────────────────────────────────────────────────────────
    async def scanner() -> None:
        nonlocal passes_done
        async with HLClient(quota=quota) as c:
            # expose client to query_one via closure
            nonlocal client
            client = c

            while time.monotonic() < deadline:
                tasks = [
                    asyncio.create_task(query_one(addr))
                    for addr in addresses
                ]
                await asyncio.gather(*tasks)
                passes_done += 1
                log.info(
                    "[pass %d done] %d addresses, %d queries total",
                    passes_done, len(addresses), queries_completed,
                )
                if time.monotonic() >= deadline:
                    break

    client: HLClient  # forward-declare for closure

    log.info("Starting smoke test — deadline in %ds …", DURATION_S)
    wall_start = time.monotonic()

    try:
        await asyncio.wait_for(
            asyncio.gather(scanner(), status_printer()),
            timeout=DURATION_S + 5,   # 5s grace beyond hard deadline
        )
    except asyncio.TimeoutError:
        log.info("Hard deadline reached — stopping.")
    except asyncio.CancelledError:
        log.info("Cancelled.")

    wall_elapsed = time.monotonic() - wall_start

    # ── Save raw samples ───────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths: list[str] = []
    for idx, (addr, raw) in enumerate(raw_samples):
        p = OUTPUT_DIR / f"smoke_raw_response_{idx}.json"
        p.write_text(json.dumps({"address": addr, "response": raw}, indent=2))
        saved_paths.append(str(p))

    # ── Diagnostics ────────────────────────────────────────────────────────────
    final_stats  = registry.stats()
    final_quota  = quota.stats()
    rl_429_count = _rl_counter.count - rl_count_at_start

    def pct(n: int, d: int) -> str:
        return f"{n/d*100:.1f}%" if d else "n/a"

    def percentile(data: list[float], p: int) -> str:
        if not data:
            return "n/a"
        data_s = sorted(data)
        idx_f  = (p / 100) * (len(data_s) - 1)
        lo, hi = int(idx_f), min(int(idx_f) + 1, len(data_s) - 1)
        val    = data_s[lo] + (data_s[hi] - data_s[lo]) * (idx_f - lo)
        return f"{val:.1f}"

    total_queries = success_count + failure_count
    throughput    = total_queries / wall_elapsed if wall_elapsed > 0 else 0

    report_lines = [
        "",
        "=" * 70,
        "  PHASE 1 SMOKE TEST DIAGNOSTIC REPORT",
        f"  Duration: {wall_elapsed:.0f}s | Addresses loaded: {len(addresses)}",
        f"  Full passes completed: {passes_done}",
        "=" * 70,
        "",
        "── A. QUOTA BEHAVIOUR ────────────────────────────────────────────────",
        f"  Total queries:          {total_queries}",
        f"  Total weight consumed:  ~{total_queries * 2}  (weight=2 each)",
        f"  Avg weight/min:         {total_queries * 2 / (wall_elapsed / 60):.0f}",
        f"  Server-side 429s:       {rl_429_count}",
        f"  Peak utilization:       {peak_utilization * 100:.1f}%",
        f"  Final quota snapshot:   usage={final_quota['usage_per_min']} "
          f"max={final_quota['max_per_min']} "
          f"util={final_quota['utilization']:.1%}",
        "",
        "── B. HLCLIENT REAL RESPONSES ────────────────────────────────────────",
        f"  Success (non-None):     {success_count}  ({pct(success_count, total_queries)})",
        f"  Failures (None):        {failure_count}  ({pct(failure_count, total_queries)})",
        f"    of which 429:         {rl_429_count}",
        f"    other (timeout/err):  {max(0, failure_count - rl_429_count)}",
        f"  Raw samples saved:      {len(saved_paths)}",
        *[f"    {p}" for p in saved_paths],
        "",
        "  assetPositions length distribution:",
        *[f"    len={k}: {v} ({pct(v, success_count)})"
          for k, v in sorted(ap_len_dist.items())],
        f"  liquidationPx=null:     {liq_px_null}/{liq_px_total} "
          f"({pct(liq_px_null, liq_px_total)})",
        f"  leverage.type values:   {dict(leverage_types)}",
        "",
        "── C. ADDRESS REGISTRY DISTRIBUTION ─────────────────────────────────",
        f"  Addresses scanned:      {len(scanned_addrs)} / {len(addresses)}",
        f"  Registry total:         {final_stats['total']}",
        f"  Tier distribution:      {final_stats['by_tier']}",
        f"  has_position == True:   {final_stats['active_holders']} "
          f"({pct(final_stats['active_holders'], final_stats['total'])})",
        "",
        "  Position USD distribution (addresses with has_position=True):",
        f"    count:  {len(position_usd_values)}",
        f"    P50:    ${float(percentile(position_usd_values, 50)):>12,.0f}"
          if position_usd_values else "    (no data)",
        f"    P90:    ${float(percentile(position_usd_values, 90)):>12,.0f}"
          if position_usd_values else "",
        f"    P99:    ${float(percentile(position_usd_values, 99)):>12,.0f}"
          if position_usd_values else "",
        f"    max:    ${max(position_usd_values):>12,.0f}"
          if position_usd_values else "",
        "",
        "── D. PERFORMANCE ────────────────────────────────────────────────────",
        f"  Total queries / elapsed: {total_queries} / {wall_elapsed:.0f}s",
        f"  Throughput:              {throughput:.2f} queries/s  "
          f"= {throughput*60:.0f} queries/min",
        f"  Addresses scanned in 10min: {len(scanned_addrs)}",
        "",
        "  clearinghouse_state latency (ms):",
        f"    P50:  {percentile(latencies_ms, 50)} ms",
        f"    P95:  {percentile(latencies_ms, 95)} ms",
        f"    max:  {max(latencies_ms):.1f} ms" if latencies_ms else "    (no data)",
        "",
        "=" * 70,
    ]

    # filter out empty strings from conditional lines
    report = "\n".join(line for line in report_lines if line is not None)
    print(report)

    log.info("Smoke test complete. Raw samples: %s", saved_paths)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    addresses = load_addresses(ADDRESSES_FILE)
    log.info("Loaded %d addresses from %s", len(addresses), ADDRESSES_FILE)
    asyncio.run(run_smoke(addresses))
