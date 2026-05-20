# HL Liquidation Hunter - Working Rules

## Project Goal
Build a Hyperliquid liquidation density tracker for statistical edge validation.
Final goal: determine if "liquidation cluster + price crossing" has predictive
edge over a fair baseline.

## Core Principles
1. **Validation before strategy.** Do not build trading logic until edge is
   statistically confirmed (p<0.01, edge>10pp over baseline).
2. **One module at a time.** Complete and test a module before starting the next.
3. **Cheap experiments first.** Always try the smallest verification before
   building infrastructure. See docs/charter.md for the standard workflow.

## Coding Standards
- Python 3.11+, type hints required on all public functions
- async/await for all I/O (aiohttp, websockets)
- pandas + pyarrow for storage
- All config in src/hl_liq_hunter/config.py, no magic numbers in logic code
- Logging via stdlib logging, format: "%(asctime)s %(levelname)s %(name)s %(message)s"

## Testing Discipline
- Every core/* module needs a corresponding test_*.py
- Tests must run without network (use mocks)
- Run `pytest tests/` after every module completion

## API Notes (CRITICAL - READ docs/api_notes.md FIRST)
- HL clearinghouseState endpoint weight = 2 (NOT 1)
- HL global limit = 1200 weight/min per IP, use 1000 as safe budget
- HL WebSocket trades schema: VERIFY field names before parsing
- HL has 200+ perp symbols, do not subscribe all in one connection

## Anti-patterns to Avoid
- DO NOT poll all addresses indiscriminately (will hit rate limit)
- DO NOT store every snapshot as a separate file (batch by minute)
- DO NOT trust online tutorials about HL API - many are outdated
- DO NOT skip the feasibility check before building collector

## When You're Stuck
- Add to docs/error-log.md with: symptom, root cause, fix
- Re-read docs/charter.md to confirm you're on the right path