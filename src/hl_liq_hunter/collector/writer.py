"""
Phase 2 — Module 2: SnapshotWriter.

Buffers parse_positions rows and periodically flushes to partitioned parquet.

Partition layout:
    {data_root}/snapshots/date=YYYY-MM-DD/hour=HH/batch_HHMMSS_{n}.parquet

WARNING: Multiple SnapshotWriter instances targeting the same data_root will
produce file-name collisions.  Use exactly one instance per process.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from hl_liq_hunter.config import DATA_ROOT

logger = logging.getLogger(__name__)


class SnapshotWriter:
    """
    Async-safe buffer + parquet sink for position snapshots.

    add() is non-blocking unless a flush is already in progress (lock contention
    is expected to be short — bounded by the duration of one to_parquet call).
    All parquet I/O happens in asyncio.to_thread so the event loop is never
    blocked.
    """

    def __init__(
        self,
        data_root: Optional[Path] = None,
        flush_interval_sec: float = 60.0,
        max_buffer_rows: int = 10_000,
    ) -> None:
        self._data_root: Path = data_root if data_root is not None else DATA_ROOT
        self._flush_interval_sec = flush_interval_sec
        self._max_buffer_rows = max_buffer_rows

        self._buffer: list[dict[str, object]] = []
        self._lock = asyncio.Lock()
        # Per-(date, hour) file counter; resets each hour naturally via the key.
        self._file_counters: dict[tuple[str, str], int] = {}

        self._last_flush_at: float = 0.0
        self._total_flushed_rows: int = 0
        self._total_files_written: int = 0

    async def add(self, rows: list[dict[str, object]]) -> None:
        """Append rows to buffer; flush immediately if max_buffer_rows is reached."""
        if not rows:
            return
        async with self._lock:
            self._buffer.extend(rows)
            if len(self._buffer) >= self._max_buffer_rows:
                await self._do_flush()

    async def flush(self) -> int:
        """
        Force-flush the buffer to parquet.

        Returns the number of rows written (0 if buffer was empty or write failed).
        On write failure the buffer is preserved for the next flush attempt.
        """
        async with self._lock:
            return await self._do_flush()

    async def run_flusher(self) -> None:
        """
        Background task: flush every flush_interval_sec seconds.

        On CancelledError performs a final flush before propagating, so that rows
        buffered at shutdown time are not lost.
        """
        try:
            while True:
                await asyncio.sleep(self._flush_interval_sec)
                await self.flush()
        except asyncio.CancelledError:
            await self.flush()
            raise

    def stats(self) -> dict[str, object]:
        """Lock-free snapshot of writer state (safe to call at any time)."""
        return {
            "buffer_size":         len(self._buffer),
            "last_flush_at":       self._last_flush_at,
            "total_flushed_rows":  self._total_flushed_rows,
            "total_files_written": self._total_files_written,
        }

    # ── internal ──────────────────────────────────────────────────────────────

    async def _do_flush(self) -> int:
        """Flush buffer. MUST be called with self._lock already held."""
        if not self._buffer:
            return 0

        rows = list(self._buffer)
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        time_str = now.strftime("%H%M%S")

        key = (date_str, hour_str)
        n = self._file_counters.get(key, 0)
        self._file_counters[key] = n + 1

        out_dir = (
            self._data_root / "snapshots"
            / f"date={date_str}"
            / f"hour={hour_str}"
        )
        out_path = out_dir / f"batch_{time_str}_{n}.parquet"

        try:
            await asyncio.to_thread(self._write_parquet_file, rows, out_dir, out_path)
        except Exception:
            logger.error(
                "SnapshotWriter flush failed — buffer preserved (%d rows), path=%s",
                len(rows),
                out_path,
                exc_info=True,
            )
            return 0

        self._buffer.clear()
        self._total_flushed_rows += len(rows)
        self._total_files_written += 1
        self._last_flush_at = time.time()
        return len(rows)

    @staticmethod
    def _write_parquet_file(
        rows: list[dict[str, object]],
        out_dir: Path,
        out_path: Path,
    ) -> None:
        """Blocking I/O — called via asyncio.to_thread."""
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(out_path, compression="zstd", index=False)
