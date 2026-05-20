"""
One-shot script: verify clearinghouseState numeric field type consistency.

Samples 200 random addresses, queries HL, inspects raw Python types of each
numeric field without parsing.  Outputs a markdown report.

Usage:
    python scripts/verify_schema_types.py
"""

from __future__ import annotations

import asyncio
import logging
import random
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Add src to path so imports work without an editable install.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hl_liq_hunter.core.hl_client import HLClient
from hl_liq_hunter.core.quota_manager import QuotaManager
from hl_liq_hunter.config import PHASE2_RATE_BUDGET, PHASE2_SCANNER_CONCURRENCY

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

ADDRESSES_FILE = Path(__file__).parent / "manual_addresses.txt"
OUTPUT_DIR = Path(__file__).parent.parent / "output"
SAMPLE_SIZE = 200
CONCURRENCY = PHASE2_SCANNER_CONCURRENCY

# Fields to inspect.  None means the field itself is the leaf value.
# Tuples indicate nested path: (outer_key, inner_key)
FIELDS: list[str] = [
    "liquidationPx",
    "positionValue",
    "entryPx",
    "szi",
    "marginUsed",
    "unrealizedPnl",
    "leverage.value",
    "leverage.rawUsd",
    "cumFunding.allTime",
    "returnOnEquity",
]


def get_field_value(pos: dict[str, object], field: str) -> object:
    """Extract a (possibly nested) field from a position dict."""
    if "." in field:
        outer, inner = field.split(".", 1)
        outer_val = pos.get(outer)
        if not isinstance(outer_val, dict):
            return "<missing>"
        return outer_val.get(inner, "<missing>")
    return pos.get(field, "<missing>")


async def query_batch(
    addresses: list[str],
    quota: QuotaManager,
    sem: asyncio.Semaphore,
) -> list[dict[str, object]]:
    """Query clearinghouseState for all addresses; return list of raw responses."""
    results: list[dict[str, object]] = []

    async def _query(addr: str) -> None:
        async with sem:
            async with HLClient(quota) as client:
                state = await client.clearinghouse_state(addr)
            if state is not None:
                results.append(state)

    await asyncio.gather(*[_query(addr) for addr in addresses])
    return results


async def main() -> None:
    # ── load addresses ─────────────────────────────────────────────────────────
    raw_lines = ADDRESSES_FILE.read_text().splitlines()
    addresses = [ln.strip() for ln in raw_lines if ln.strip().startswith("0x")]
    sample = random.sample(addresses, min(SAMPLE_SIZE, len(addresses)))
    print(f"Querying {len(sample)} addresses …", flush=True)

    # ── query ─────────────────────────────────────────────────────────────────
    quota = QuotaManager(max_per_min=PHASE2_RATE_BUDGET)
    sem = asyncio.Semaphore(CONCURRENCY)
    states = await query_batch(sample, quota, sem)
    print(f"Got {len(states)} responses", flush=True)

    # ── inspect types ─────────────────────────────────────────────────────────
    counters: dict[str, Counter[str]] = {f: Counter() for f in FIELDS}
    total_positions = 0

    for state in states:
        positions = state.get("assetPositions", [])
        if not isinstance(positions, list):
            continue
        for item in positions:
            if not isinstance(item, dict):
                continue
            pos = item.get("position")
            if not isinstance(pos, dict):
                continue
            # skip zero-size positions so we count only real data
            szi_raw = pos.get("szi", "0")
            try:
                if float(szi_raw) == 0.0:  # type: ignore[arg-type]
                    continue
            except (TypeError, ValueError):
                pass
            total_positions += 1
            for field in FIELDS:
                val = get_field_value(pos, field)
                if val == "<missing>":
                    counters[field]["<missing>"] += 1
                else:
                    counters[field][type(val).__name__] += 1

    # ── build report ──────────────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"schema_types_{ts}.md"

    lines: list[str] = []
    lines.append("# Schema Type Verification")
    lines.append("")
    lines.append(f"Total addresses queried: {len(sample)}")
    lines.append(f"Addresses with valid response: {len(states)}")
    lines.append(f"Total non-zero positions inspected: {total_positions}")
    lines.append("")
    lines.append("| Field | str | int | float | None | other | Missing | Mixed? |")
    lines.append("|-------|-----|-----|-------|------|-------|---------|--------|")

    conclusions_consistent: list[str] = []
    conclusions_mixed: list[str] = []

    for field in FIELDS:
        c = counters[field]
        n_str     = c.get("str",     0)
        n_int     = c.get("int",     0)
        n_float   = c.get("float",   0)
        n_none    = c.get("NoneType", 0)
        n_missing = c.get("<missing>", 0)
        other_keys = set(c.keys()) - {"str", "int", "float", "NoneType", "<missing>"}
        n_other = sum(c[k] for k in other_keys)

        value_types = {k for k in c if k not in ("<missing>",) and c[k] > 0}
        # "mixed" = more than one non-None value type present
        non_null_types = value_types - {"NoneType"}
        mixed = "Y" if len(non_null_types) > 1 else "N"

        lines.append(
            f"| {field} | {n_str} | {n_int} | {n_float} | {n_none} "
            f"| {n_other} | {n_missing} | {mixed} |"
        )

        dominant = non_null_types.pop() if len(non_null_types) == 1 else None
        if mixed == "N" and dominant:
            conclusions_consistent.append(f"- **{field}**: consistently `{dominant}`")
        elif mixed == "Y":
            breakdown = ", ".join(f"`{k}`={c[k]}" for k in sorted(non_null_types | {dominant or ""}))
            conclusions_mixed.append(f"- **{field}**: MIXED — {breakdown}")

    lines.append("")
    lines.append("## Conclusion")
    lines.append("")
    if conclusions_consistent:
        lines.append("### Consistent fields")
        lines.extend(conclusions_consistent)
    lines.append("")
    if conclusions_mixed:
        lines.append("### Mixed-type fields (parser must handle both)")
        lines.extend(conclusions_mixed)
    else:
        lines.append("No mixed-type fields detected.")

    report = "\n".join(lines)
    out_path.write_text(report)
    print(f"\nReport written to {out_path}\n")
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
