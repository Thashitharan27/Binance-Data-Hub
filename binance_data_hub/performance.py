"""Runtime download telemetry and persistent performance history."""
from __future__ import annotations

import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock


class TransferMeter:
    """Thread-safe transfer meter with rolling speed, peak speed and file ETA."""

    def __init__(self, callback=None, emit_interval: float = 0.5, rolling_window: float = 3.0):
        self.callback = callback
        self.emit_interval = max(0.1, float(emit_interval))
        self.rolling_window = max(0.5, float(rolling_window))
        self.lock = Lock()
        self.started_monotonic = time.monotonic()
        self.started_at = datetime.now(timezone.utc)
        self.network_bytes = 0
        self.completed_files = 0
        self.total_files = 0
        self.peak_bps = 0.0
        self.last_emit = self.started_monotonic
        self.samples = deque([(self.started_monotonic, 0)])

    def _snapshot_locked(self, now: float) -> dict:
        elapsed = max(0.000001, now - self.started_monotonic)
        while len(self.samples) > 1 and self.samples[0][0] < now - self.rolling_window:
            self.samples.popleft()
        sample_time, sample_bytes = self.samples[0]
        sample_elapsed = max(0.000001, now - sample_time)
        current_bps = max(0.0, (self.network_bytes - sample_bytes) / sample_elapsed)
        self.peak_bps = max(self.peak_bps, current_bps)
        average_bps = self.network_bytes / elapsed
        files_per_minute = self.completed_files / elapsed * 60.0
        eta_seconds = None
        if self.completed_files > 0 and self.total_files > self.completed_files:
            eta_seconds = elapsed / self.completed_files * (self.total_files - self.completed_files)
        return {
            "elapsed_seconds": elapsed,
            "network_bytes": int(self.network_bytes),
            "current_bps": current_bps,
            "average_bps": average_bps,
            "peak_bps": self.peak_bps,
            "current_mbps": current_bps * 8.0 / 1_000_000.0,
            "average_mbps": average_bps * 8.0 / 1_000_000.0,
            "peak_mbps": self.peak_bps * 8.0 / 1_000_000.0,
            "completed_files": int(self.completed_files),
            "total_files": int(self.total_files),
            "files_per_minute": files_per_minute,
            "eta_seconds": eta_seconds,
        }

    def _maybe_emit(self, force: bool = False):
        callback = None
        snapshot = None
        now = time.monotonic()
        with self.lock:
            if not force and now - self.last_emit < self.emit_interval:
                return
            self.last_emit = now
            snapshot = self._snapshot_locked(now)
            callback = self.callback
        if callback:
            callback(snapshot)

    def add_bytes(self, amount: int):
        if amount <= 0:
            return
        now = time.monotonic()
        with self.lock:
            self.network_bytes += int(amount)
            self.samples.append((now, self.network_bytes))
            while len(self.samples) > 1 and self.samples[0][0] < now - self.rolling_window:
                self.samples.popleft()
        self._maybe_emit()

    def mark_files(self, completed: int, total: int):
        with self.lock:
            self.completed_files = max(self.completed_files, int(completed))
            self.total_files = max(self.total_files, int(total))
        self._maybe_emit(force=True)

    def finish(self, completed: int | None = None, total: int | None = None) -> dict:
        if completed is not None or total is not None:
            with self.lock:
                if completed is not None:
                    self.completed_files = max(self.completed_files, int(completed))
                if total is not None:
                    self.total_files = max(self.total_files, int(total))
        self._maybe_emit(force=True)
        with self.lock:
            return self._snapshot_locked(time.monotonic())


def _ensure_run_table(db):
    db.execute(
        """CREATE TABLE IF NOT EXISTS download_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            elapsed_seconds REAL NOT NULL,
            symbols TEXT NOT NULL,
            datasets TEXT NOT NULL,
            intervals TEXT NOT NULL,
            max_connections INTEGER NOT NULL,
            segments INTEGER NOT NULL,
            verify INTEGER NOT NULL,
            planned_files INTEGER NOT NULL,
            processed_files INTEGER NOT NULL,
            downloaded_files INTEGER NOT NULL,
            skipped_files INTEGER NOT NULL,
            missing_files INTEGER NOT NULL,
            failed_files INTEGER NOT NULL,
            segmented_files INTEGER NOT NULL,
            network_bytes INTEGER NOT NULL,
            average_mbps REAL NOT NULL,
            peak_mbps REAL NOT NULL,
            files_per_minute REAL NOT NULL
        )"""
    )


def record_run_history(root, *, meter: TransferMeter, summary: dict, symbols, datasets, intervals, max_connections: int, segments: int, verify: bool):
    root = Path(root)
    metrics = summary.get("performance", {})
    counts = summary.get("counts", {})
    finished_at = datetime.now(timezone.utc)
    with sqlite3.connect(root / "manifest.sqlite") as db:
        _ensure_run_table(db)
        db.execute(
            """INSERT INTO download_runs(
                started_at,finished_at,elapsed_seconds,symbols,datasets,intervals,
                max_connections,segments,verify,planned_files,processed_files,
                downloaded_files,skipped_files,missing_files,failed_files,segmented_files,
                network_bytes,average_mbps,peak_mbps,files_per_minute
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                meter.started_at.isoformat(),
                finished_at.isoformat(),
                float(metrics.get("elapsed_seconds", 0.0)),
                ",".join(symbols),
                ",".join(datasets),
                ",".join(intervals),
                int(max_connections),
                int(segments),
                1 if verify else 0,
                int(summary.get("planned", 0)),
                int(summary.get("files", 0)),
                int(counts.get("downloaded", 0)),
                int(counts.get("skipped", 0)),
                int(counts.get("missing", 0)),
                int(counts.get("failed", 0)),
                int(summary.get("segmented_files", 0)),
                int(metrics.get("network_bytes", 0)),
                float(metrics.get("average_mbps", 0.0)),
                float(metrics.get("peak_mbps", 0.0)),
                float(metrics.get("files_per_minute", 0.0)),
            ),
        )


def recent_run_history(root, limit: int = 10) -> list[dict]:
    path = Path(root) / "manifest.sqlite"
    if not path.exists():
        return []
    with sqlite3.connect(path) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='download_runs'").fetchone()
        if not exists:
            return []
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """SELECT id,started_at,elapsed_seconds,max_connections,segments,planned_files,
                      downloaded_files,skipped_files,missing_files,failed_files,segmented_files,
                      network_bytes,average_mbps,peak_mbps,files_per_minute
               FROM download_runs ORDER BY id DESC LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall()
    return [dict(row) for row in rows]
