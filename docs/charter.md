# HL Liquidation Hunter — Project Charter

## Hypothesis
When a significant liquidation cluster forms near the current price on Hyperliquid,
the probability of price visiting that level within N candles is meaningfully higher
than a fair baseline (random walk expectation at the same distance).

## Definition of "Edge"
- **Threshold**: p < 0.01 (chi-squared or binomial test), edge > 10 percentage points
  over baseline hit-rate.
- **Baseline**: historical price-touch rate for the same distance/time window
  with *no* liquidation cluster present.

### Sampling Caveat (updated 2026-05-20)
Our density map is built from **active trader addresses** (harvested via WS trades stream),
not a random sample of all HL accounts. This is an intentional bias:
- Active accounts ↔ higher leverage ↔ real near-term liquidation targets.
- The edge we are testing is specifically:
  **"active-account liquidation cluster + price crossing"** — not full-market density.
- Implication for Phase 4 validation: baseline must be computed on the same
  active-account sample, not global OI. Do not conflate the two.

### Cluster Threshold (updated 2026-05-20)
- Original charter: $20M — **too strict for active-account sample**.
- Revised working threshold: **$2–5M** (active-account density scale).
- Final threshold to be calibrated empirically in Phase 4 against hit-rate curves.

---

## Standard Workflow (Cheap → Expensive)

### Step 0 — Feasibility Probe — **PASSED (Weak GO)** ✓
> Completed 2026-05-20. 2033 active-trader addresses, BTC ±5% liquidation
> density $13.1M, clear short cluster at +2.75% ($3.99M) and long cluster at
> -2.25% ($2.56M). Visual correlation with 24h price action observed in charts.
> Proceeding to Phase 1 with sampling caveat and revised cluster threshold.
1. Pull a small REST snapshot of open interest / liquidation data for 1 symbol.
2. Confirm field names, data shape, and rate-limit weights by hand.
3. Record findings in `docs/api_notes.md`.
4. **Gate**: If the data doesn't contain what we need → stop and reassess.

**Status: PASSED (Weak GO) — 2026-05-20**

### Step 1 — Build Core Infrastructure — **PASSED** ✓
> Completed 2026-05-20. 30/30 tests, mypy --strict + ruff clean across all modules.
> - **QuotaManager** (`core/quota_manager.py`): 9/9 tests, 127 lines.
>   Sliding-window rate limiter, 50ms polling, lock released before sleep (no convoy).
> - **AddressRegistry** (`core/address_registry.py`): 12/12 tests, 275 lines.
>   Tier state machine (WHALE/MIDTIER/LONGTAIL), dirty-flag TTL, parquet persistence,
>   asyncio.to_thread for non-blocking I/O, FakeClock-injectable for deterministic tests.
>   Design note: dirty TTL (300s) << LONGTAIL interval (3600s); Phase 2 dirty scanner
>   must drain dirty queue within 5min or signals silently expire (logged in error-log.md).
> - **HLClient** (`core/hl_client.py`): 9/9 tests, 185 lines.
>   Weight-aware (clearinghouseState=2, candleSnapshot=20, allMids=2),
>   all errors → Optional[...] pattern, no retry (caller decides), aioresponses mock.
1. Find/scrape at least 30 days of per-symbol trade + funding snapshots.
2. Reconstruct a crude "liquidation density" proxy (e.g. OI change spikes).
3. Label events and run the contingency test.
4. **Gate**: p ≥ 0.01 → hypothesis rejected, don't build live infra.

### Step 2 — Live Collector (only if Step 1 passes)
> Rate-limit configuration validated via 5-config burst experiment (2026-05-20).
> Baseline: `PHASE2_RATE_BUDGET=900`, `PHASE2_SCANNER_CONCURRENCY=8` per scanner,
> `PHASE2_INTER_BATCH_SLEEP_SEC=1.0`.  Root cause of 429s: burst speed + post-429
> sleep accumulation (H2+H3 combined).  See docs/error-log.md for full analysis.
1. Build `core/ws_collector.py` to stream trades + liquidation events.
2. Build `core/liq_density.py` to maintain a rolling density map.
3. Store to `data/raw/` as minutely parquet batches.
4. **Gate**: 7 days of clean data required before proceeding.

### Step 3 — Signal Generator (only if Step 2 data exists)
1. Build `core/signal.py` — detect cluster formation, emit cross events.
2. Run `analysis/edge_test.py` on live-collected data.
3. **Gate**: Same statistical threshold as Step 1.

### Step 4 — Strategy Prototype (only if Step 3 passes)
- Out of scope until gates 1–3 are cleared.

---

## Module Build Order
```
core/hl_client.py       # thin REST/WS wrapper, rate-limit aware
core/liq_density.py     # density map data structure
core/ws_collector.py    # live streaming + persistence
analysis/backtest.py    # historical edge test (Step 1)
analysis/edge_test.py   # live-data edge test (Step 3)
```

## Key Constraints
- No live trading code until edge is confirmed.
- Rate budget: **900 weight/min** for Phase 2 (HL hard limit 1200; see burst experiment).
- Storage: parquet, batched per minute, never one file per event.
