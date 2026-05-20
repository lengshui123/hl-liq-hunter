"""
Quota burst controlled experiment — Phase 1 → Phase 2 calibration.

Tests 5 configurations (max_per_min × concurrency) for 5 min each to
determine the true cause of 429s observed in the Phase 1 smoke test.

Three hypotheses under test
---------------------------
H1 – Window drift:     client's rolling window drifts from HL's; same
                       budget but different start → server sees overage.
                       Signal: config_B (1000/min, concurrency=5 — low
                       burst rate) still produces 429s.

H2 – Sleep accumulation: when a coroutine sleeps 8 s after 429, the
                          other 14 concurrent coroutines keep consuming
                          weight, causing the next 60-s window to also
                          overspend.
                          Signal: 429s arrive in rapid succession
                          (cluster width < 1 s).

H3 – Sub-minute burst: HL limits instantaneous rate independent of
                        60-s total. concurrency=15 @ ~90 ms RTT →
                        burst ≈ 333 weight/s, which may trigger a
                        per-second or per-10-s sublimit.
                        Signal: D (700/15) has more 429s than A (700/5)
                        despite same 60-s budget.

Run order: A → B → C → D → E, 30 s gap between configs.
Total wall time: ≈ 25 min 30 s.

Usage:
    python scripts/quota_burst_experiment.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, cast

import aiohttp

from hl_liq_hunter.config import HL_INFO_URL
from hl_liq_hunter.core.address_registry import AddressRegistry
from hl_liq_hunter.core.hl_client import HLClient, _RATE_LIMIT_SLEEP_S
from hl_liq_hunter.core.quota_manager import QuotaManager

# ── Config ────────────────────────────────────────────────────────────────────

ADDRESSES_FILE  = Path(__file__).parent / "manual_addresses.txt"
RUN_DURATION_S  = 300   # 5 minutes per config
GAP_S           = 30    # pause between configs
PROGRESS_EVERY  = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
# Suppress hl_client WARNING noise; we capture 429s ourselves
logging.getLogger("hl_liq_hunter.core.hl_client").setLevel(logging.ERROR)
log = logging.getLogger("quota_experiment")


@dataclass
class ExperimentConfig:
    name: str
    max_per_min: int
    concurrency: int


CONFIGS: list[ExperimentConfig] = [
    ExperimentConfig("A", max_per_min=700,  concurrency=5),
    ExperimentConfig("B", max_per_min=1000, concurrency=5),
    ExperimentConfig("C", max_per_min=850,  concurrency=8),
    ExperimentConfig("D", max_per_min=700,  concurrency=15),
    ExperimentConfig("E", max_per_min=1000, concurrency=15),
]


# ── Per-call stats collector ──────────────────────────────────────────────────

@dataclass
class CallRecord:
    acquire_ms:  float
    network_ms:  float
    is_429:      bool
    timestamp:   float   # monotonic time of the call start


@dataclass
class ConfigResult:
    cfg:          ExperimentConfig
    records:      list[CallRecord] = field(default_factory=list)
    elapsed_s:    float = 0.0

    # derived
    @property
    def total(self) -> int:
        return len(self.records)

    @property
    def success(self) -> int:
        return sum(1 for r in self.records if not r.is_429)

    @property
    def count_429(self) -> int:
        return sum(1 for r in self.records if r.is_429)

    @property
    def avg_weight_per_min(self) -> float:
        if self.elapsed_s == 0:
            return 0.0
        return self.total * 2 / (self.elapsed_s / 60)

    def percentile(self, values: list[float], p: int) -> str:
        if not values:
            return "n/a"
        s = sorted(values)
        idx = (p / 100) * (len(s) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
        v = s[lo] + (s[hi] - s[lo]) * (idx - lo)
        return f"{v:.1f}"

    def p50_rtt(self) -> str:
        return self.percentile([r.network_ms for r in self.records if not r.is_429], 50)

    def p95_rtt(self) -> str:
        return self.percentile([r.network_ms for r in self.records if not r.is_429], 95)

    def p50_acquire(self) -> str:
        return self.percentile([r.acquire_ms for r in self.records], 50)

    def p95_acquire(self) -> str:
        return self.percentile([r.acquire_ms for r in self.records], 95)

    def cluster_429_widths_ms(self) -> list[float]:
        """
        Time gaps between consecutive 429s (ms).
        Gaps < 1000ms indicate burst clustering (H2 signal).
        """
        ts = sorted(r.timestamp for r in self.records if r.is_429)
        return [(ts[i+1] - ts[i]) * 1000 for i in range(len(ts) - 1)]

    def time_in_window_of_429s(self) -> list[float]:
        """
        For each 429, what fraction of the 300s run had elapsed?
        Helps see if 429s cluster at the start of each 60s window.
        """
        if self.elapsed_s == 0:
            return []
        t0 = min(r.timestamp for r in self.records) if self.records else 0.0
        return [((r.timestamp - t0) % 60) for r in self.records if r.is_429]


# ── Instrumented HLClient subclass (no Phase 1 code modified) ────────────────

class _InstrumentedClient(HLClient):
    """
    Subclass of HLClient that overrides _post to separately time
    quota acquisition vs network RTT.  Logic is identical to parent;
    only instrumentation is added.
    """

    def __init__(
        self,
        quota: QuotaManager,
        result: ConfigResult,
        base_url: str = HL_INFO_URL,
    ) -> None:
        super().__init__(quota=quota, base_url=base_url)
        self._result = result

    async def _post(
        self,
        endpoint_name: str,
        payload: dict[str, object],
        weight: int,
    ) -> Optional[object]:
        assert self._session is not None

        call_start = time.monotonic()

        # ── measure acquire wait ──────────────────────────────────────────────
        t_acquire_start = time.monotonic()
        await self._quota.acquire(weight)
        acquire_ms = (time.monotonic() - t_acquire_start) * 1000

        # ── measure network RTT ───────────────────────────────────────────────
        t_net_start = time.monotonic()
        is_429 = False
        result: Optional[object] = None

        try:
            async with self._session.post(
                self._base_url, json=payload
            ) as resp:
                if resp.status == 429:
                    is_429 = True
                    await asyncio.sleep(_RATE_LIMIT_SLEEP_S)
                elif resp.status >= 400:
                    pass  # result stays None
                else:
                    try:
                        result = cast(object, await resp.json(content_type=None))
                    except Exception:
                        pass
        except asyncio.TimeoutError:
            pass
        except aiohttp.ClientError:
            pass

        network_ms = (time.monotonic() - t_net_start) * 1000

        self._result.records.append(CallRecord(
            acquire_ms=acquire_ms,
            network_ms=network_ms,
            is_429=is_429,
            timestamp=call_start,
        ))

        return result


# ── Single-config runner ──────────────────────────────────────────────────────

async def run_config(
    cfg: ExperimentConfig,
    addresses: list[str],
) -> ConfigResult:
    result = ConfigResult(cfg=cfg)
    deadline = time.monotonic() + RUN_DURATION_S

    quota    = QuotaManager(max_per_min=cfg.max_per_min)
    registry = AddressRegistry()
    for addr in addresses:
        await registry.add_address(addr)

    sem = asyncio.Semaphore(cfg.concurrency)
    queries_done = 0

    async def query_one(addr: str) -> None:
        nonlocal queries_done
        async with sem:
            if time.monotonic() >= deadline:
                return
            result_val = await client.clearinghouse_state(addr)

            # Update registry so tier logic is exercised
            if result_val and isinstance(result_val, dict):
                positions = result_val.get("assetPositions", [])
                total_usd = 0.0
                has_pos = False
                for p in positions:
                    try:
                        v = float(p.get("position", {}).get("positionValue", 0) or 0)
                        if v > 0:
                            has_pos = True
                            total_usd += abs(v)
                    except (ValueError, TypeError):
                        pass
                await registry.update_after_scan(
                    addr, total_usd=total_usd,
                    has_position=has_pos, scanned_at=time.time(),
                )

            queries_done += 1
            if queries_done % PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - (deadline - RUN_DURATION_S)
                q429 = result.count_429
                log.info(
                    "[%s] %d queries | %.0fs | 429s=%d | quota %.0f%%",
                    cfg.name, queries_done, elapsed, q429,
                    quota.stats()["utilization"] * 100,
                )

    wall_start = time.monotonic()

    async with _InstrumentedClient(quota=quota, result=result) as client:
        pass_n = 0
        while time.monotonic() < deadline:
            addrs_this_pass = list(addresses)
            tasks = [asyncio.create_task(query_one(a)) for a in addrs_this_pass]
            await asyncio.gather(*tasks)
            pass_n += 1

    result.elapsed_s = time.monotonic() - wall_start
    log.info(
        "[%s] DONE — %d queries, %d×429, %.0fs elapsed",
        cfg.name, result.total, result.count_429, result.elapsed_s,
    )
    return result


# ── Address loader ────────────────────────────────────────────────────────────

def load_addresses(path: Path) -> list[str]:
    addrs = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            addrs.append(line)
    return addrs


# ── Report generator ──────────────────────────────────────────────────────────

def print_report(results: list[ConfigResult]) -> None:
    sep = "=" * 72

    print(f"\n{sep}")
    print("  QUOTA BURST EXPERIMENT — RESULTS")
    print(sep)

    # ── Table 1: main metrics ─────────────────────────────────────────────────
    print("""
┌──────┬────────┬───────┬────────┬──────────┬───────────────┬──────────────┐
│ Cfg  │ budget │ conc  │ total  │ 429 cnt  │ weight/min    │ p50/p95 RTT  │
├──────┼────────┼───────┼────────┼──────────┼───────────────┼──────────────┤""")
    for r in results:
        print(
            f"│  {r.cfg.name}   │ "
            f"{r.cfg.max_per_min:>5}  │ "
            f"{r.cfg.concurrency:>4}  │ "
            f"{r.total:>6}  │ "
            f"{r.count_429:>6}    │ "
            f"{r.avg_weight_per_min:>10.0f}    │ "
            f"{r.p50_rtt():>5}/{r.p95_rtt():>5} ms  │"
        )
    print("└──────┴────────┴───────┴────────┴──────────┴───────────────┴──────────────┘")

    # ── Table 2: acquire latency ───────────────────────────────────────────────
    print("""
  Acquire wait latency (ms) — time spent blocked in QuotaManager.acquire():
┌──────┬──────────────────────────────┐
│ Cfg  │  p50 acquire / p95 acquire   │
├──────┼──────────────────────────────┤""")
    for r in results:
        print(f"│  {r.cfg.name}   │  {r.p50_acquire():>8} / {r.p95_acquire():>8} ms      │")
    print("└──────┴──────────────────────────────┘")

    # ── Table 3: 429 timing detail ─────────────────────────────────────────────
    print("\n  429 timing detail:")
    for r in results:
        if r.count_429 == 0:
            print(f"  [{r.cfg.name}] 0 × 429 — no data")
            continue
        widths = r.cluster_429_widths_ms()
        in_window = r.time_in_window_of_429s()
        tight_clusters = sum(1 for w in widths if w < 1000)
        print(
            f"  [{r.cfg.name}] {r.count_429} × 429  |  "
            f"inter-429 gaps < 1s: {tight_clusters}/{max(len(widths),1)}  |  "
            f"position-in-60s-window: "
            f"min={min(in_window):.1f}s max={max(in_window):.1f}s "
            f"mean={sum(in_window)/len(in_window):.1f}s"
        )

    # ── Hypothesis judgements ─────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  HYPOTHESIS JUDGEMENTS")
    print(sep)

    # H1: window drift — does config_B (1000/5) produce 429s?
    b = next((r for r in results if r.cfg.name == "B"), None)
    h1_signal = b and b.count_429 > 0
    print(f"""
H1 — Window drift (client rolling window drifts from HL's):
  Config B (1000/min, conc=5) → {b.count_429 if b else "n/a"} × 429
  Verdict: {"SUPPORTED — 429s at low concurrency imply timing drift" if h1_signal
            else "NOT SUPPORTED — Config B is clean; drift not the primary cause"}""")

    # H2: sleep accumulation — 429s cluster tightly?
    h2_data = [(r.cfg.name, r.cluster_429_widths_ms()) for r in results if r.count_429 >= 2]
    tight_total = sum(sum(1 for w in ws if w < 1000) for _, ws in h2_data)
    h2_signal   = tight_total > 0
    print(f"""
H2 — Sleep accumulation (other coroutines consume during 8s sleep):
  Tight clusters (gap < 1s) across all configs: {tight_total}
  Verdict: {"SUPPORTED — consecutive 429s in rapid succession indicate pile-up" if h2_signal
            else "NOT SUPPORTED — no tight 429 clustering observed"}""")

    # H3: burst rate — does higher concurrency increase 429s at same budget?
    a = next((r for r in results if r.cfg.name == "A"), None)
    d = next((r for r in results if r.cfg.name == "D"), None)
    e = next((r for r in results if r.cfg.name == "E"), None)

    h3_ad = (d and a and d.count_429 > a.count_429)
    h3_be = (e and b and e.count_429 > b.count_429)
    h3_signal = h3_ad or h3_be
    print(f"""
H3 — Sub-minute burst rate (HL limits instantaneous rate, not just 60-s total):
  A(700/5): {a.count_429 if a else "n/a"} × 429   D(700/15): {d.count_429 if d else "n/a"} × 429   (same budget, diff concurrency)
  B(1000/5): {b.count_429 if b else "n/a"} × 429   E(1000/15): {e.count_429 if e else "n/a"} × 429
  Verdict: {"SUPPORTED — higher concurrency = more 429s at equal budget" if h3_signal
            else "NOT SUPPORTED — concurrency had no significant effect on 429 rate"}""")

    # ── Phase 2 recommendation ────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  PHASE 2 CONFIGURATION RECOMMENDATION")
    print(sep)

    # Find config with zero (or fewest) 429s and highest throughput
    clean = [r for r in results if r.count_429 == 0]
    best  = max(clean, key=lambda r: r.total) if clean else min(results, key=lambda r: r.count_429)

    print(f"""
  Best config by data: {best.cfg.name}  (max_per_min={best.cfg.max_per_min}, concurrency={best.cfg.concurrency})
    429s: {best.count_429}  |  throughput: {best.total / best.elapsed_s:.2f} q/s  |  p95 RTT: {best.p95_rtt()} ms

  Suggested PHASE2_RATE_BUDGET:       {best.cfg.max_per_min}
  Suggested PHASE2_SCANNER_CONCURRENCY: {best.cfg.concurrency}
  Inter-query sleep: {"not needed — no 429s at this config" if best.count_429 == 0
                      else "consider 0.1s/query to flatten burst"}
""")
    print(sep)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    addresses_master = load_addresses(ADDRESSES_FILE)
    log.info("Loaded %d addresses", len(addresses_master))

    results: list[ConfigResult] = []

    for i, cfg in enumerate(CONFIGS):
        if i > 0:
            log.info("Gap: sleeping %ds before config %s …", GAP_S, cfg.name)
            await asyncio.sleep(GAP_S)

        addrs = list(addresses_master)
        random.shuffle(addrs)

        log.info(
            "━━━ Starting config %s: max_per_min=%d, concurrency=%d ━━━",
            cfg.name, cfg.max_per_min, cfg.concurrency,
        )
        result = await run_config(cfg, addrs)
        results.append(result)

    print_report(results)


if __name__ == "__main__":
    asyncio.run(main())
