"""High-throughput mirror for Binance USD-M Futures public archives.

The hub stores Binance's official ZIP files without extracting or merging them.
That makes collection substantially faster and avoids turning one download job
into a second large disk-I/O job. Downstream research tools can materialize only
the subsets they actually need.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from .catalog import DATASETS, DatasetSpec, INTERVALS


ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
USER_AGENT = "Binance-Data-Hub/3.0"
CHUNK_SIZE = 1024 * 1024
DEFAULT_WORKERS = 16
MAX_WORKERS = 64


@dataclass(frozen=True)
class ArchiveTask:
    dataset: str
    symbol: str
    period: str
    key: str
    interval: str | None
    url: str
    relative_path: Path
    span_start: date
    span_end: date


@dataclass
class DownloadResult:
    task: ArchiveTask
    status: str
    bytes: int = 0
    sha256: str | None = None
    error: str | None = None


def _as_date(value, default: date) -> date:
    if value in (None, ""):
        return default
    return pd.Timestamp(value).date()


def _month_start(day: date) -> date:
    return day.replace(day=1)


def _next_month(day: date) -> date:
    return (pd.Timestamp(day) + pd.offsets.MonthBegin(1)).date()


def _month_end(day: date) -> date:
    return _next_month(_month_start(day)) - timedelta(days=1)


def _filename(spec: DatasetSpec, symbol: str, interval: str | None, key: str) -> str:
    if spec.intervalled:
        return f"{symbol}-{interval}-{key}.zip"
    return f"{symbol}-{spec.key}-{key}.zip"


def _relative_path(spec: DatasetSpec, symbol: str, period: str, interval: str | None, filename: str) -> Path:
    path = Path("raw") / "futures" / "um" / period / spec.key / symbol
    if spec.intervalled:
        path /= str(interval)
    return path / filename


def _task(spec: DatasetSpec, symbol: str, period: str, key: str, interval: str | None, span_start: date, span_end: date) -> ArchiveTask:
    filename = _filename(spec, symbol, interval, key)
    parts = [ARCHIVE_BASE, period, spec.key, symbol]
    if spec.intervalled:
        parts.append(str(interval))
    url = "/".join(parts + [filename])
    return ArchiveTask(spec.key, symbol, period, key, interval, url, _relative_path(spec, symbol, period, interval, filename), span_start, span_end)


def plan_archive_tasks(
    symbol: str,
    datasets: list[str] | tuple[str, ...],
    intervals: list[str] | tuple[str, ...],
    start_date=None,
    end_date=None,
    *,
    today: date | None = None,
) -> list[ArchiveTask]:
    """Plan the smallest set of official archive files for the requested range.

    Completed historical months use one monthly ZIP whenever Binance offers it.
    Daily files are used for daily-only datasets and for the current partial
    month. Today is excluded because Binance daily archives arrive after the day
    has completed.
    """
    symbol = symbol.strip().upper().replace("/", "")
    if not symbol or not symbol.isalnum():
        raise ValueError("Enter a valid USD-M symbol such as BTCUSDT.")
    unknown = [key for key in datasets if key not in DATASETS]
    if unknown:
        raise ValueError(f"Unknown datasets: {unknown}")
    invalid_intervals = [item for item in intervals if item not in INTERVALS]
    if invalid_intervals:
        raise ValueError(f"Unsupported intervals: {invalid_intervals}")

    today = today or datetime.now(timezone.utc).date()
    last_published_day = today - timedelta(days=1)
    start = _as_date(start_date, date(2020, 1, 1))
    end = min(_as_date(end_date, last_published_day), last_published_day)
    if start > end:
        return []

    current_month = _month_start(today)
    planned: list[ArchiveTask] = []

    for dataset in datasets:
        spec = DATASETS[dataset]
        dataset_intervals = intervals if spec.intervalled else [None]
        for interval in dataset_intervals:
            if spec.monthly:
                month = _month_start(start)
                while month < current_month and month <= end:
                    month_last = _month_end(month)
                    if month_last >= start:
                        span_start = max(start, month)
                        span_end = min(end, month_last)
                        planned.append(_task(spec, symbol, "monthly", month.strftime("%Y-%m"), interval, span_start, span_end))
                    month = _next_month(month)

                if spec.daily and end >= current_month:
                    day = max(start, current_month)
                    while day <= end:
                        planned.append(_task(spec, symbol, "daily", day.isoformat(), interval, day, day))
                        day += timedelta(days=1)
            elif spec.daily:
                day = start
                while day <= end:
                    planned.append(_task(spec, symbol, "daily", day.isoformat(), interval, day, day))
                    day += timedelta(days=1)

    planned.sort(key=lambda item: (item.dataset, item.interval or "", item.span_start, item.period))
    return planned


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest().lower()


def _fetch_checksum(task: ArchiveTask, opener, retries: int = 4) -> str | None:
    url = f"{task.url}.CHECKSUM"
    for attempt in range(retries):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with opener(request, timeout=30) as response:
                text = response.read().decode("utf-8-sig").strip()
            return text.split()[0].lower() if text else None
        except HTTPError as exc:
            if exc.code == 404:
                return None
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
            time.sleep(float(exc.headers.get("Retry-After", min(2**attempt, 8))))
        except URLError:
            if attempt == retries - 1:
                raise
            time.sleep(min(2**attempt, 8))
    return None


def _download_one(task: ArchiveTask, root: Path, *, verify: bool, cancelled, opener, retries: int = 5) -> DownloadResult:
    final = root / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)

    if final.exists() and final.stat().st_size > 0:
        if not verify:
            return DownloadResult(task, "skipped", final.stat().st_size)
        expected = _fetch_checksum(task, opener)
        if expected and _sha256(final) == expected:
            return DownloadResult(task, "skipped", final.stat().st_size, expected)

    part = final.with_name(f"{final.name}.part")
    for attempt in range(retries):
        if cancelled and cancelled():
            return DownloadResult(task, "cancelled", part.stat().st_size if part.exists() else 0)

        resume_from = part.stat().st_size if part.exists() else 0
        headers = {"User-Agent": USER_AGENT}
        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"
        request = Request(task.url, headers=headers)

        try:
            with opener(request, timeout=120) as response:
                status = getattr(response, "status", None)
                if status is None and hasattr(response, "getcode"):
                    status = response.getcode()
                append = bool(resume_from and status == 206)
                if resume_from and not append:
                    resume_from = 0
                mode = "ab" if append else "wb"
                with part.open(mode) as output:
                    while True:
                        if cancelled and cancelled():
                            return DownloadResult(task, "cancelled", part.stat().st_size if part.exists() else 0)
                        block = response.read(CHUNK_SIZE)
                        if not block:
                            break
                        output.write(block)
            if not part.exists() or not part.stat().st_size:
                raise RuntimeError("Binance returned an empty archive.")

            digest = None
            if verify:
                expected = _fetch_checksum(task, opener)
                if expected is None:
                    raise RuntimeError("Checksum file is missing.")
                digest = _sha256(part)
                if digest != expected:
                    part.unlink(missing_ok=True)
                    raise RuntimeError(f"SHA-256 mismatch: expected {expected}, got {digest}")

            os.replace(part, final)
            return DownloadResult(task, "downloaded", final.stat().st_size, digest)

        except HTTPError as exc:
            if exc.code == 404:
                part.unlink(missing_ok=True)
                return DownloadResult(task, "missing", 0, error="HTTP 404")
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                return DownloadResult(task, "failed", part.stat().st_size if part.exists() else 0, error=f"HTTP {exc.code}")
            time.sleep(float(exc.headers.get("Retry-After", min(2**attempt, 10))))
        except (URLError, OSError, RuntimeError) as exc:
            if attempt == retries - 1:
                return DownloadResult(task, "failed", part.stat().st_size if part.exists() else 0, error=str(exc))
            time.sleep(min(2**attempt, 10))

    return DownloadResult(task, "failed", error="retry limit reached")


class Manifest:
    def __init__(self, path: Path):
        self.path = path
        self.lock = Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS archives (
                    dataset TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    key TEXT NOT NULL,
                    interval TEXT NOT NULL DEFAULT '',
                    relative_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    bytes INTEGER NOT NULL DEFAULT 0,
                    sha256 TEXT,
                    error TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(dataset, symbol, period, key, interval)
                )"""
            )

    def record(self, result: DownloadResult) -> None:
        task = result.task
        with self.lock, sqlite3.connect(self.path) as db:
            db.execute(
                """INSERT INTO archives(dataset,symbol,period,key,interval,relative_path,status,bytes,sha256,error,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(dataset,symbol,period,key,interval) DO UPDATE SET
                     relative_path=excluded.relative_path,status=excluded.status,bytes=excluded.bytes,
                     sha256=excluded.sha256,error=excluded.error,updated_at=excluded.updated_at""",
                (
                    task.dataset, task.symbol, task.period, task.key, task.interval or "",
                    str(task.relative_path), result.status, int(result.bytes), result.sha256,
                    result.error, datetime.now(timezone.utc).isoformat(),
                ),
            )


def _daily_fallback_tasks(task: ArchiveTask) -> list[ArchiveTask]:
    spec = DATASETS[task.dataset]
    if task.period != "monthly" or not spec.daily:
        return []
    tasks = []
    day = task.span_start
    while day <= task.span_end:
        tasks.append(_task(spec, task.symbol, "daily", day.isoformat(), task.interval, day, day))
        day += timedelta(days=1)
    return tasks


def download_archive_library(
    symbols: str | list[str] | tuple[str, ...],
    datasets: list[str] | tuple[str, ...],
    intervals: list[str] | tuple[str, ...],
    root: str | Path,
    start_date=None,
    end_date=None,
    *,
    workers: int = DEFAULT_WORKERS,
    verify: bool = False,
    progress=None,
    cancelled=None,
    opener=urlopen,
    today: date | None = None,
) -> dict:
    """Mirror requested public archives with concurrent streaming downloads."""
    workers = int(workers)
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"workers must be between 1 and {MAX_WORKERS}")

    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(root / "manifest.sqlite")

    if isinstance(symbols, str):
        symbol_list = [item for item in symbols.replace(",", " ").split() if item]
    else:
        symbol_list = list(symbols)
    normalized_symbols = []
    for symbol in symbol_list:
        cleaned = symbol.strip().upper().replace("/", "")
        if cleaned and cleaned not in normalized_symbols:
            normalized_symbols.append(cleaned)
    if not normalized_symbols:
        raise ValueError("Enter at least one USD-M symbol.")

    tasks = []
    for symbol in normalized_symbols:
        tasks.extend(plan_archive_tasks(symbol, datasets, intervals, start_date, end_date, today=today))
    tasks.sort(key=lambda item: (item.symbol, item.dataset, item.interval or "", item.span_start, item.period))
    results: list[DownloadResult] = []

    def run_batch(batch: list[ArchiveTask], completed_base: int, total_hint: int) -> list[DownloadResult]:
        batch_results: list[DownloadResult] = []
        if not batch:
            return batch_results
        with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as pool:
            futures = {
                pool.submit(_download_one, task, root, verify=verify, cancelled=cancelled, opener=opener): task
                for task in batch
            }
            for index, future in enumerate(as_completed(futures), 1):
                result = future.result()
                manifest.record(result)
                batch_results.append(result)
                if progress:
                    progress(completed_base + index, total_hint, result)
                if cancelled and cancelled():
                    for pending in futures:
                        pending.cancel()
                    break
        return batch_results

    primary = run_batch(tasks, 0, len(tasks))
    results.extend(primary)

    fallbacks: list[ArchiveTask] = []
    for result in primary:
        if result.status == "missing":
            fallbacks.extend(_daily_fallback_tasks(result.task))

    existing = {(t.dataset, t.symbol, t.period, t.key, t.interval) for t in tasks}
    fallback_map = {}
    for task in fallbacks:
        identity = (task.dataset, task.symbol, task.period, task.key, task.interval)
        if identity not in existing:
            fallback_map[identity] = task
    fallbacks = list(fallback_map.values())

    if fallbacks and not (cancelled and cancelled()):
        fallback_results = run_batch(fallbacks, len(primary), len(tasks) + len(fallbacks))
        results.extend(fallback_results)

    counts = {status: sum(1 for item in results if item.status == status) for status in ("downloaded", "skipped", "missing", "failed", "cancelled")}
    return {
        "root": str(root),
        "planned": len(tasks),
        "fallbacks": len(fallbacks),
        "files": len(results),
        "bytes_downloaded": sum(item.bytes for item in results if item.status == "downloaded"),
        "counts": counts,
        "results": results,
    }
