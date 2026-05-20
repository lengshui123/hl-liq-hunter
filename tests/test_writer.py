"""
Tests for collector/writer.py — SnapshotWriter.
No network I/O.  All parquet writes go to pytest's tmp_path.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest

from hl_liq_hunter.collector.writer import SnapshotWriter

# ── helpers ───────────────────────────────────────────────────────────────────

_SAMPLE_ROW: dict[str, object] = {
    "address":      "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "symbol":       "BTC",
    "timestamp_ms": 1_779_252_000_000,
    "side":         "long",
    "size_usd":     50_000.0,
    "entry_px":     65_000.0,
    "liq_px":       60_000.0,
    "leverage":     5.0,
    "lev_type":     "cross",
}


def make_rows(n: int) -> list[dict[str, object]]:
    return [dict(_SAMPLE_ROW) for _ in range(n)]


def all_parquet(root: Path) -> list[Path]:
    return sorted((root / "snapshots").rglob("*.parquet"))


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_add_below_buffer_no_flush(tmp_path: Path) -> None:
    """add() below max_buffer_rows does not trigger a flush."""
    w = SnapshotWriter(data_root=tmp_path, max_buffer_rows=100)
    await w.add(make_rows(5))
    s = w.stats()
    assert s["buffer_size"] == 5
    assert s["total_files_written"] == 0
    assert s["total_flushed_rows"] == 0
    assert not (tmp_path / "snapshots").exists()


async def test_add_triggers_flush_at_max_buffer(tmp_path: Path) -> None:
    """add() that reaches max_buffer_rows triggers an immediate flush."""
    w = SnapshotWriter(data_root=tmp_path, max_buffer_rows=10)
    await w.add(make_rows(10))
    s = w.stats()
    assert s["buffer_size"] == 0
    assert s["total_files_written"] == 1
    assert s["total_flushed_rows"] == 10
    assert len(all_parquet(tmp_path)) == 1


async def test_manual_flush(tmp_path: Path) -> None:
    """flush() writes buffered rows and clears the buffer."""
    w = SnapshotWriter(data_root=tmp_path)
    await w.add(make_rows(3))
    written = await w.flush()
    assert written == 3
    assert w.stats()["buffer_size"] == 0
    assert w.stats()["total_flushed_rows"] == 3
    assert len(all_parquet(tmp_path)) == 1


async def test_flush_empty_buffer_no_file(tmp_path: Path) -> None:
    """flush() on an empty buffer returns 0 and creates no files or directories."""
    w = SnapshotWriter(data_root=tmp_path)
    written = await w.flush()
    assert written == 0
    assert not (tmp_path / "snapshots").exists()


async def test_partition_structure(tmp_path: Path) -> None:
    """Flushed files land under snapshots/date=.../hour=.../batch_*.parquet."""
    w = SnapshotWriter(data_root=tmp_path)
    await w.add(make_rows(2))
    await w.flush()
    files = all_parquet(tmp_path)
    assert len(files) == 1
    path = files[0]
    parts = path.parts
    assert any(p.startswith("date=") for p in parts), f"no date= in {parts}"
    assert any(p.startswith("hour=") for p in parts), f"no hour= in {parts}"
    assert path.name.startswith("batch_"), f"unexpected name {path.name}"
    assert path.suffix == ".parquet"


async def test_parquet_readable(tmp_path: Path) -> None:
    """Written parquet is readable and has the expected columns + row count."""
    w = SnapshotWriter(data_root=tmp_path)
    await w.add(make_rows(5))
    await w.flush()
    df = pd.read_parquet(all_parquet(tmp_path)[0])
    assert len(df) == 5
    expected_cols = {
        "address", "symbol", "timestamp_ms", "side",
        "size_usd", "entry_px", "liq_px", "leverage", "lev_type",
    }
    assert expected_cols.issubset(set(df.columns)), f"missing cols: {expected_cols - set(df.columns)}"


async def test_concurrent_add_safe(tmp_path: Path) -> None:
    """100 concurrent add() calls produce no data loss or double-counting."""
    w = SnapshotWriter(data_root=tmp_path, max_buffer_rows=10_000)
    tasks = [asyncio.create_task(w.add(make_rows(1))) for _ in range(100)]
    await asyncio.gather(*tasks)
    await w.flush()
    s = w.stats()
    assert s["total_flushed_rows"] == 100
    assert s["buffer_size"] == 0
    # Verify via actual file content
    dfs = [pd.read_parquet(f) for f in all_parquet(tmp_path)]
    total_in_files = sum(len(df) for df in dfs)
    assert total_in_files == 100


async def test_run_flusher_periodic(tmp_path: Path) -> None:
    """run_flusher triggers a flush after flush_interval_sec."""
    w = SnapshotWriter(data_root=tmp_path, flush_interval_sec=0.1)
    await w.add(make_rows(3))
    task = asyncio.create_task(w.run_flusher())
    await asyncio.sleep(0.25)  # long enough for at least one 0.1s interval
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cast(int, w.stats()["total_flushed_rows"]) >= 3


async def test_flusher_cancel_flushes_remaining(tmp_path: Path) -> None:
    """Cancelling run_flusher flushes any buffered rows before propagating."""
    w = SnapshotWriter(data_root=tmp_path, flush_interval_sec=60.0)
    await w.add(make_rows(5))
    task = asyncio.create_task(w.run_flusher())
    await asyncio.sleep(0)  # yield: task starts, blocks at asyncio.sleep(60)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert w.stats()["total_flushed_rows"] == 5
    assert w.stats()["buffer_size"] == 0
    assert len(all_parquet(tmp_path)) == 1


async def test_flush_failure_preserves_buffer(tmp_path: Path, monkeypatch: Any) -> None:
    """If the parquet write raises, the buffer is NOT cleared."""

    def _bad_write(
        rows: list[dict[str, object]], out_dir: Path, out_path: Path
    ) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(SnapshotWriter, "_write_parquet_file", staticmethod(_bad_write))

    w = SnapshotWriter(data_root=tmp_path)
    await w.add(make_rows(3))
    written = await w.flush()

    assert written == 0
    assert w.stats()["buffer_size"] == 3
    assert w.stats()["total_files_written"] == 0
    assert not (tmp_path / "snapshots").exists()
