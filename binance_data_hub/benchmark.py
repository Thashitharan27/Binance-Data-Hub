"""Adaptive Binance CDN speed benchmark and connection auto-tuning."""
from __future__ import annotations

import json
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Event, Lock
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .archive_downloader import ARCHIVE_BASE, USER_AGENT

DEFAULT_BENCHMARK_LEVELS = (4, 8, 16, 24, 32)
DEFAULT_BENCHMARK_SECONDS = 15
BENCHMARK_BLOCK = 64 * 1024
BENCHMARK_RANGE = 8 * 1024 * 1024
RECOMMENDATION_FRACTION = 0.95


@dataclass(frozen=True)
class BenchmarkSource:
    url: str
    size: int
    symbol: str
    key: str


class _Counter:
    def __init__(self):
        self.lock = Lock()
        self.bytes = 0
        self.errors = 0

    def add_bytes(self, amount: int):
        with self.lock:
            self.bytes += int(amount)

    def add_error(self):
        with self.lock:
            self.errors += 1

    def snapshot(self):
        with self.lock:
            return self.bytes, self.errors


def _previous_months(today: date, count: int = 6):
    year, month = today.year, today.month
    values = []
    for _ in range(count):
        month -= 1
        if month == 0:
            year -= 1
            month = 12
        values.append(f"{year:04d}-{month:02d}")
    return values


def _candidate_urls(symbol: str, today: date):
    symbols = []
    normalized = symbol.strip().upper().replace("/", "") or "BTCUSDT"
    for item in (normalized, "BTCUSDT"):
        if item not in symbols:
            symbols.append(item)
    for sym in symbols:
        for key in _previous_months(today):
            yield sym, key, f"{ARCHIVE_BASE}/monthly/aggTrades/{sym}/{sym}-aggTrades-{key}.zip"
            yield sym, key, f"{ARCHIVE_BASE}/monthly/trades/{sym}/{sym}-trades-{key}.zip"
            yield sym, key, f"{ARCHIVE_BASE}/monthly/klines/{sym}/1m/{sym}-1m-{key}.zip"


def _probe_source(url: str, opener=urlopen):
    request = Request(url, headers={"User-Agent": USER_AGENT, "Range": "bytes=0-0"})
    try:
        with opener(request, timeout=15) as response:
            status = getattr(response, "status", None)
            if status is None and hasattr(response, "getcode"):
                status = response.getcode()
            if status != 206:
                return None
            headers = getattr(response, "headers", {}) or {}
            content_range = str(headers.get("Content-Range") or headers.get("content-range") or "")
            match = re.search(r"/(\d+)$", content_range)
            if not match:
                return None
            size = int(match.group(1))
            response.read(1)
            return size if size > BENCHMARK_RANGE else None
    except (HTTPError, URLError, OSError, TimeoutError):
        return None


def find_benchmark_source(symbol: str = "BTCUSDT", *, opener=urlopen, today: date | None = None) -> BenchmarkSource:
    today = today or datetime.now(timezone.utc).date()
    for source_symbol, key, url in _candidate_urls(symbol, today):
        size = _probe_source(url, opener)
        if size:
            return BenchmarkSource(url=url, size=size, symbol=source_symbol, key=key)
    raise RuntimeError("Could not find a recent Binance archive with HTTP Range support for the benchmark.")


def _worker(source: BenchmarkSource, worker_id: int, connection_count: int, deadline: float, counter: _Counter, stop: Event, opener, cancelled):
    round_index = 0
    usable = max(1, source.size - BENCHMARK_RANGE)
    while not stop.is_set() and time.monotonic() < deadline and not (cancelled and cancelled()):
        start = ((worker_id + round_index * connection_count) * BENCHMARK_RANGE) % usable
        end = min(source.size - 1, start + BENCHMARK_RANGE - 1)
        request = Request(source.url, headers={"User-Agent": USER_AGENT, "Range": f"bytes={start}-{end}"})
        try:
            with opener(request, timeout=8) as response:
                status = getattr(response, "status", None)
                if status is None and hasattr(response, "getcode"):
                    status = response.getcode()
                if status != 206:
                    counter.add_error()
                    return
                while not stop.is_set() and time.monotonic() < deadline and not (cancelled and cancelled()):
                    block = response.read(BENCHMARK_BLOCK)
                    if not block:
                        break
                    counter.add_bytes(len(block))
        except (HTTPError, URLError, OSError, TimeoutError):
            counter.add_error()
            if time.monotonic() >= deadline:
                return
            time.sleep(0.15)
        round_index += 1


def _run_level(source: BenchmarkSource, connections: int, seconds: float, *, progress=None, opener=urlopen, cancelled=None):
    connections = max(1, int(connections))
    seconds = max(3.0, float(seconds))
    counter = _Counter()
    stop = Event()
    started = time.monotonic()
    deadline = started + seconds

    with ThreadPoolExecutor(max_workers=connections) as pool:
        futures = [
            pool.submit(_worker, source, i, connections, deadline, counter, stop, opener, cancelled)
            for i in range(connections)
        ]
        last_bytes = 0
        last_time = started
        while time.monotonic() < deadline and not (cancelled and cancelled()):
            time.sleep(0.5)
            now = time.monotonic()
            total_bytes, errors = counter.snapshot()
            delta_t = max(0.000001, now - last_time)
            current_mbps = max(0.0, (total_bytes - last_bytes) * 8.0 / delta_t / 1_000_000.0)
            elapsed = now - started
            if progress:
                progress({
                    "connections": connections,
                    "elapsed_seconds": min(elapsed, seconds),
                    "target_seconds": seconds,
                    "network_bytes": total_bytes,
                    "current_mbps": current_mbps,
                    "errors": errors,
                })
            last_bytes, last_time = total_bytes, now
        stop.set()
        for future in futures:
            try:
                future.result(timeout=10)
            except Exception:
                counter.add_error()

    elapsed = max(0.000001, time.monotonic() - started)
    total_bytes, errors = counter.snapshot()
    average_mbps = total_bytes * 8.0 / elapsed / 1_000_000.0
    return {
        "connections": connections,
        "elapsed_seconds": elapsed,
        "network_bytes": total_bytes,
        "average_mbps": average_mbps,
        "average_mb_s": total_bytes / elapsed / 1024 / 1024,
        "errors": errors,
    }


def recommend_connections(results: list[dict]) -> dict:
    valid = [item for item in results if item.get("network_bytes", 0) > 0]
    if not valid:
        raise RuntimeError("Benchmark did not receive usable data from Binance.")
    best = max(valid, key=lambda item: item["average_mbps"])
    threshold = best["average_mbps"] * RECOMMENDATION_FRACTION
    healthy = [item for item in valid if item["average_mbps"] >= threshold and item.get("errors", 0) == 0]
    if not healthy:
        healthy = [item for item in valid if item["average_mbps"] >= threshold]
    recommended = min(healthy, key=lambda item: item["connections"]) if healthy else best
    return {
        "recommended_connections": int(recommended["connections"]),
        "recommended_mbps": float(recommended["average_mbps"]),
        "best_connections": int(best["connections"]),
        "best_mbps": float(best["average_mbps"]),
        "threshold_fraction": RECOMMENDATION_FRACTION,
        "efficiency_pct": 100.0 * recommended["average_mbps"] / max(best["average_mbps"], 0.000001),
    }


def _ensure_table(db):
    db.execute(
        """CREATE TABLE IF NOT EXISTS speed_benchmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            source_url TEXT NOT NULL,
            seconds_per_level REAL NOT NULL,
            levels TEXT NOT NULL,
            recommended_connections INTEGER NOT NULL,
            recommended_mbps REAL NOT NULL,
            best_connections INTEGER NOT NULL,
            best_mbps REAL NOT NULL,
            results_json TEXT NOT NULL
        )"""
    )


def _record(root, started_at, source, seconds_per_level, levels, recommendation, results):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(root / "manifest.sqlite") as db:
        _ensure_table(db)
        db.execute(
            """INSERT INTO speed_benchmarks(
                started_at,finished_at,symbol,source_url,seconds_per_level,levels,
                recommended_connections,recommended_mbps,best_connections,best_mbps,results_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                started_at.isoformat(), datetime.now(timezone.utc).isoformat(), source.symbol, source.url,
                float(seconds_per_level), ",".join(str(x) for x in levels),
                recommendation["recommended_connections"], recommendation["recommended_mbps"],
                recommendation["best_connections"], recommendation["best_mbps"],
                json.dumps(results, separators=(",", ":")),
            ),
        )


def recent_benchmark_history(root, limit: int = 5) -> list[dict]:
    path = Path(root) / "manifest.sqlite"
    if not path.exists():
        return []
    with sqlite3.connect(path) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='speed_benchmarks'").fetchone()
        if not exists:
            return []
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """SELECT id,started_at,symbol,seconds_per_level,recommended_connections,
                      recommended_mbps,best_connections,best_mbps,results_json
               FROM speed_benchmarks ORDER BY id DESC LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall()
    return [dict(row) for row in rows]


def benchmark_connections(root, symbol: str = "BTCUSDT", *, levels=DEFAULT_BENCHMARK_LEVELS, seconds_per_level: float = DEFAULT_BENCHMARK_SECONDS, progress=None, opener=urlopen, cancelled=None, today: date | None = None):
    levels = tuple(sorted({max(1, int(x)) for x in levels}))
    if not levels:
        raise ValueError("Choose at least one connection level.")
    seconds_per_level = max(3.0, float(seconds_per_level))
    started_at = datetime.now(timezone.utc)
    source = find_benchmark_source(symbol, opener=opener, today=today)
    results = []
    total = len(levels)

    for index, connections in enumerate(levels, 1):
        if cancelled and cancelled():
            return {"cancelled": True, "source": source, "results": results}

        def stage_progress(snapshot):
            if progress:
                progress(index, total, snapshot)

        result = _run_level(source, connections, seconds_per_level, progress=stage_progress, opener=opener, cancelled=cancelled)
        results.append(result)
        if progress:
            progress(index, total, {**result, "stage_complete": True})

    recommendation = recommend_connections(results)
    _record(root, started_at, source, seconds_per_level, levels, recommendation, results)
    return {"cancelled": False, "source": source, "results": results, **recommendation}
