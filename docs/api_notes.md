# HL API Notes

> Fill this in during Step 0 feasibility probe and update as you discover new behaviour.
> **Do not trust external tutorials — verify every field name yourself.**

---

## Info Endpoints Used

Base URL (confirmed): `https://api.hyperliquid.xyz/info`
Transport: `POST`, `Content-Type: application/json` body.
All requests share the same URL — endpoint is identified by the `"type"` field in the JSON body.

| Endpoint | Payload `type` | Weight | Notes |
|----------|----------------|--------|-------|
| `POST /info` | `meta` | 1 | All perp symbols + decimals |
| `POST /info` | `allMids` | **2** | Mid prices for all symbols; prices returned as **strings** — must `float()` convert |
| `POST /info` | `clearinghouseState` | **2** | Per-address positions (NOT 1 — confirmed Phase 0) |
| `POST /info` | `candleSnapshot` | **20** | OHLCV candles; Phase 1 simplified weight — refine in Phase 4 |
| `POST /info` | `openOrders` | 1 | Per-address open orders |
| *(add more as discovered)* | | | |

### Request body schemas (confirmed)

```jsonc
// clearinghouseState
{"type": "clearinghouseState", "user": "0xADDRESS"}

// allMids
{"type": "allMids"}

// candleSnapshot
{
  "type": "candleSnapshot",
  "req": {
    "coin": "BTC",
    "interval": "1m",      // e.g. "1m", "5m", "1h", "1d"
    "startTime": 1700000000000,  // unix ms
    "endTime":   1700000060000   // unix ms
  }
}
```

### 429 behaviour
- HTTP status 429, body: `b"Too Many Requests"` (plain text, no JSON).
- HLClient sleeps 8s then returns None; no retry (caller decides).
- In practice, the batch strategy (250 req × weight 2 + 30s sleep) has produced zero 429s on 2033 addresses (confirmed 2026-05-20).

---

## Rate Limit Rules

- **Hard limit**: 1200 weight / min per IP
- **Safe budget**: use **1000** weight / min in code (`config.HL_RATE_BUDGET`)
- Weight resets on a rolling 60-second window (not clock-aligned minute)
- Crossing the limit returns HTTP 429; back off exponentially
- `clearinghouseState` costs **2**, not 1 — easy to blow budget if polling many addresses
- **Practical batch strategy** (confirmed 2026-05-20): 250 req/batch × weight 2 = 500 weight/batch; sleep 30s between batches → ~857 weight/min, well under limit. 2033 addresses completed without a single 429.

---

## WebSocket Schemas Observed

> Record exact field names from live messages. Do not guess from docs.

### Connection
- URL: `wss://api.hyperliquid.xyz/ws`
- Subscription message format:
  ```json
  {"method": "subscribe", "subscription": {"type": "<channel>", ...}}
  ```

### Channel: `trades` ✓ confirmed 2026-05-20
```jsonc
{
  "channel": "trades",
  "data": [
    {
      "coin":  "BTC",
      "side":  "A",           // "A" = ask-side aggressor (taker sells); "B" = bid-side aggressor (taker buys)
      "px":    "76667.0",     // fill price, string
      "sz":    "0.33675",     // fill size in base asset, string
      "time":  1779240828092, // unix milliseconds
      "hash":  "0x...",       // transaction hash (all-zeros for some internal fills)
      "tid":   931652409408432, // trade ID, int
      "users": [
        "0xf9109ada...",      // [0] = one counterparty address
        "0xe84fbad5..."       // [1] = other counterparty address
      ]
      // NOTE: field is "users", NOT "buyer"/"seller" — sides are determined by "side" field
    }
    // data is a list; multiple fills can share the same hash (same block/tx)
  ]
}
```

### Channel: `l2Book`
```jsonc
// TODO: fill in when needed
```

### Channel: `activeAssetCtx` (liquidation-related?)
```jsonc
// TODO: fill in when needed
```

---

## clearinghouseState Response Schema (verified 200 addresses / 862 positions, 2026-05-20)

> Type verification method: `type()` on each field in live API responses (not JSON visual inspection).
> Source: `scripts/verify_schema_types.py`, output `output/schema_types_20260520_134309.md`.

```jsonc
{
  "marginSummary":              {...},
  "crossMarginSummary": {
    "accountValue":       "12345.67",   // string
    "totalNtlPos":        "...",
    "totalRawUsd":        "...",
    "totalMarginUsed":    "..."
  },
  "crossMaintenanceMarginUsed": "...",
  "withdrawable":               "...",
  "time":                       1716200000000,  // unix ms, int
  "assetPositions": [
    {
      "type": "oneWay",           // outer wrapper field — observed value "oneWay"; may vary
      "position": {
        "coin":           "BTC",
        "szi":            "0.5",          // str (negative = short)
        "leverage": {
          "type":   "cross",              // str: "cross" | "isolated"
          "value":  25,                   // int — THE ONLY NATIVE NUMBER in position fields
          "rawUsd": "12345.67"            // str — ONLY present for isolated positions (~8% of positions)
        },
        "entryPx":        "65000.0",      // str
        "positionValue":  "844640.0",     // str (CONFIRMED — NOT a float, despite appearances)
        "unrealizedPnl":  "-1234.5",      // str
        "returnOnEquity": "-4.21",        // str (CONFIRMED — NOT a float)
        "liquidationPx":  "1404.73",      // str OR null — see gotcha below
        "marginUsed":     "...",          // str
        "maxLeverage":    40,             // int
        "cumFunding": {
          "allTime":      "...",          // str
          "sinceOpen":    "...",          // str
          "sinceChange":  "..."           // str
        }
      }
    }
  ]
}
```

**Field type summary** (862 non-zero positions, 200 addresses, 2026-05-20):
| Field | Type | Notes |
|-------|------|-------|
| `szi` | `str` | |
| `entryPx` | `str` | |
| `positionValue` | `str` | Previously mis-documented as float — CORRECTED |
| `unrealizedPnl` | `str` | |
| `returnOnEquity` | `str` | Previously mis-documented as float — CORRECTED |
| `liquidationPx` | `str` \| `None` | `None` for ~44% of positions (cross, over-collateralised) |
| `marginUsed` | `str` | |
| `leverage.value` | `int` | **Only native numeric field** |
| `leverage.rawUsd` | `str` | Present only for isolated positions (~8% of sample) |
| `cumFunding.allTime` | `str` | |

**leverage.type observed distribution** (2034 addresses, Phase 1 smoke test):
- `"cross"`: 94% of positions
- `"isolated"`: 6% of positions

**assetPositions length distribution** (same sample):
- 0 positions: 16.5% of accounts
- 1 position:  47.4%
- 2 positions:  9.3%
- 3+ positions: 26.8%

---

## Known Gotchas

- `clearinghouseState` weight = **2** confirmed (1200/min hard limit → 600 req/min max for this endpoint alone; use batching with inter-batch sleep)
- WS single connection: subscribing to **>~12 symbols causes server-side disconnect** (no close frame). Shard into groups of ≤10. Confirmed stable: 10+2 split across two connections.
- `candleSnapshot` with `interval="5m"` works correctly; returns standard OHLCV with fields `{t, T, o, h, l, c, v, n}` (open_time, close_time, open, high, low, close, volume, trade_count).
- 200+ perp symbols exist — never subscribe all on a single WS connection; shard by symbol group
- `hash` field in trades can be all-zeros (`0x000...000`) for certain internal/system fills — do not use as unique key
- Field names in WS messages may differ from REST responses — verify independently
- **`liquidationPx` is `str` when non-null, `None` when null** (confirmed via `type()` across 862 positions, 2026-05-20). Previously mis-documented as `float`. ~44% of positions are null.  Root cause CONFIRMED via HL official docs.**
  - 100% of null positions are cross-margin; isolated positions always have liq_px.
  - HL official formula: `liq_price = price − side × margin_available / position_size / (1 − l × side)` where `margin_available = account_value − maintenance_margin_required` for cross.
  - When a cross-margin account's `account_value` greatly exceeds `position_size`, the formula yields a negative (long) or astronomical (short) value — HL returns null rather than a meaningless number.
  - **Null positions are by definition "extremely safe" — their contribution to liquidation density is approximately zero.**
  - **Recommendation: skip all null entries. Do NOT attempt a formula-based fallback — it requires account-level `crossMarginSummary` fields and adds no signal for density mapping.**
  - Fields needed if account-level liq_px reconstruction is ever desired: `crossMarginSummary.accountValue`, `crossMaintenanceMarginUsed`, `position.szi` (signed size), `position.entryPx`.
- **`positionValue` is a string, NOT a float** — CORRECTED 2026-05-20. Earlier mis-observation came from a small smoke-test sample where values happened to be round numbers; JSON rendering of `"844640.0"` vs `844640.0` is visually similar. Type-verified via `type()` across 862 positions. Always apply `float()` conversion.
- HL may apply sub-minute burst rate limits in addition to the rolling 60s window. Observed: 21 × 429s in 10-min smoke test at concurrency=15 even though rolling-window budget (1000/min) was not exceeded by total weight. Phase 2 baseline: concurrency=8, max_per_min=850.
