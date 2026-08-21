"""Metadata-only archive size estimator for Binance Data Hub."""
from __future__ import annotations

import re
import shutil
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .archive_downloader import USER_AGENT, ArchiveTask, _daily_fallback_tasks, plan_archive_tasks
from .benchmark import recent_benchmark_history
from .performance import recent_run_history

_CACHE_LOCK = Lock()
_MISSING_CACHE_SECONDS = 12 * 60 * 60


def _ensure_metadata_table(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with _CACHE_LOCK, sqlite3.connect(root / "manifest.sqlite") as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS archive_metadata (
                url TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                bytes INTEGER,
                checked_at TEXT NOT NULL
            )"""
        )


def _cached_probe(root: Path, url: str):
    path = root / "manifest.sqlite"
    if not path.exists():
        return None
    with _CACHE_LOCK, sqlite3.connect(path) as db:
        row = db.execute(
            "SELECT status,bytes,checked_at FROM archive_metadata WHERE url=?",
            (url,),
        ).fetchone()
    if not row:
        return None
    status, size, checked_at = row
    if status == "available":
        return status, int(size or 0)
    if status == "missing":
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(checked_at)).total_seconds()
        except (TypeError, ValueError):
            age = _MISSING_CACHE_SECONDS + 1
        if age <= _MISSING_CACHE_SECONDS:
            return status, 0
    return None


def _store_probe(root: Path, url: str, status: str, size: int | None) -> None:
    with _CACHE_LOCK, sqlite3.connect(root / "manifest.sqlite") as db:
        db.execute(
            """INSERT INTO archive_metadata(url,status,bytes,checked_at)
               VALUES(?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET
                 status=excluded.status,bytes=excluded.bytes,checked_at=excluded.checked_at""",
            (url, status, None if size is None else int(size), datetime.now(timezone.utc).isoformat()),
        )


def _parse_total_from_content_range(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"/(\d+)\s*$", str(value))
    return int(match.group(1)) if match else None


def _probe_remote_size(root: Path, task: ArchiveTask, opener=urlopen) -> tuple[str, int | None]:
    cached = _cached_probe(root, task.url)
    if cached is not None:
        return cached

    request = Request(task.url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with opener(request, timeout=25) as response:
            headers = getattr(response, "headers", {}) or {}
            raw = headers.get("Content-Length") or headers.get("content-length")
            if raw is not None:
                try:
                    size = int(raw)
                except (TypeError, ValueError):
                    size = None
                if size is not None and size >= 0:
                    _store_probe(root, task.url, "available", size)
                    return "available", size
    except HTTPError as exc:
        if exc.code == 404:
            _store_probe(root, task.url, "missing", 0)
            return "missing", 0
        if exc.code not in (400, 403, 405):
            return "error", None
    except (URLError, OSError):
        return "error", None

    # A few CDNs omit Content-Length on HEAD. Ask for only byte zero and read
    # the total from Content-Range; the response body is never consumed.
    request = Request(task.url, headers={"User-Agent": USER_AGENT, "Range": "bytes=0-0"})
    try:
        with opener(request, timeout=25) as response:
            headers = getattr(response, "headers", {}) or {}
            size = _parse_total_from_content_range(
                headers.get("Content-Range") or headers.get("content-range")
            )
            if size is None:
                raw = headers.get("Content-Length") or headers.get("content-length")
                try:
                    size = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    size = None
            if size is None:
                return "error", None
            _store_probe(root, task.url, "available", size)
            return "available", size
    except HTTPError as exc:
        if exc.code == 404:
            _store_probe(root, task.url, "missing", 0)
            return "missing", 0
        return "error", None
    except (URLError, OSError):
        return "error", None


def _partial_bytes(root: Path, task: ArchiveTask, remote_size: int) -> int:
    final = root / task.relative_path
    part = final.with_name(f"{final.name}.part")
    single = part.stat().st_size if part.exists() else 0
    segment_dir = final.with_name(f".{final.name}.segments")
    segmented = 0
    if segment_dir.exists():
        for item in segment_dir.glob("*.part"):
            try:
                segmented += item.stat().st_size
            except OSError:
                pass
    return min(remote_size, max(single, segmented))


def _estimate_one(root: Path, task: ArchiveTask, opener, cancelled, allow_fallback=True) -> list[dict]:
    if cancelled and cancelled():
        return []

    final = root / task.relative_path
    if final.exists() and final.stat().st_size > 0:
        return [{
            "task": task,
            "status": "present",
            "remote_bytes": int(final.stat().st_size),
            "local_bytes": int(final.stat().st_size),
            "remaining_bytes": 0,
        }]

    status, remote_size = _probe_remote_size(root, task, opener)
    if status == "missing" and allow_fallback:
        fallbacks = _daily_fallback_tasks(task)
        if fallbacks:
            resolved = []
            for fallback in fallbacks:
                if cancelled and cancelled():
                    break
                resolved.extend(_estimate_one(root, fallback, opener, cancelled, allow_fallback=False))
            return resolved

    if status == "available" and remote_size is not None:
        local = _partial_bytes(root, task, int(remote_size))
        remaining = max(0, int(remote_size) - local)
        return [{
            "task": task,
            "status": "partial" if local else "remaining",
            "remote_bytes": int(remote_size),
            "local_bytes": int(local),
            "remaining_bytes": int(remaining),
        }]

    return [{
        "task": task,
        "status": "unavailable" if status == "missing" else "unknown",
        "remote_bytes": 0,
        "local_bytes": 0,
        "remaining_bytes": 0,
    }]


def _recent_speed_mbps(root: Path) -> tuple[float, str]:
    candidates = []
    try:
        for row in recent_run_history(root, 1):
            speed = float(row.get("average_mbps", 0) or 0)
            if speed > 0:
                candidates.append((str(row.get("started_at", "")), speed, "latest collection average"))
    except Exception:
        pass
    try:
        for row in recent_benchmark_history(root, 1):
            speed = float(row.get("recommended_mbps", 0) or 0)
            if speed > 0:
                candidates.append((str(row.get("started_at", "")), speed, "latest Auto Tune"))
    except Exception:
        pass
    if not candidates:
        return 0.0, "no measured speed yet"
    candidates.sort(key=lambda item: item[0])
    _, speed, source = candidates[-1]
    return speed, source


def estimate_archive_library(
    symbols,
    datasets,
    intervals,
    root,
    start_date=None,
    end_date=None,
    *,
    max_connections: int = 24,
    progress=None,
    cancelled=None,
    opener=urlopen,
    today=None,
) -> dict:
    """Estimate selected Binance archives without downloading archive bodies."""
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    _ensure_metadata_table(root)

    if isinstance(symbols, str):
        raw_symbols = symbols.replace(",", " ").split()
    else:
        raw_symbols = list(symbols)
    normalized_symbols = []
    for symbol in raw_symbols:
        cleaned = str(symbol).strip().upper().replace("/", "")
        if cleaned and cleaned not in normalized_symbols:
            normalized_symbols.append(cleaned)
    if not normalized_symbols:
        raise ValueError("Enter at least one USD-M symbol.")

    tasks = []
    for symbol in normalized_symbols:
        tasks.extend(plan_archive_tasks(symbol, datasets, intervals, start_date, end_date, today=today))

    workers = max(1, min(int(max_connections), 32))
    results = []
    total = len(tasks)
    done = 0
    if total:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_estimate_one, root, task, opener, cancelled): task
                for task in tasks
            }
            for future in as_completed(futures):
                if cancelled and cancelled():
                    break
                task = futures[future]
                entries = future.result()
                results.extend(entries)
                done += 1
                if progress:
                    progress(done, total, task, entries)

    by_dataset = {}
    present_bytes = 0
    partial_bytes = 0
    remaining_bytes = 0
    present_files = partial_files = remaining_files = unavailable_files = unknown_files = 0
    for item in results:
        dataset = item["task"].dataset
        bucket = by_dataset.setdefault(dataset, {
            "files": 0,
            "present_files": 0,
            "needed_files": 0,
            "unavailable_files": 0,
            "unknown_files": 0,
            "remaining_bytes": 0,
        })
        bucket["files"] += 1
        status = item["status"]
        if status == "present":
            present_files += 1
            present_bytes += item["local_bytes"]
            bucket["present_files"] += 1
        elif status == "partial":
            partial_files += 1
            partial_bytes += item["local_bytes"]
            remaining_bytes += item["remaining_bytes"]
            bucket["needed_files"] += 1
            bucket["remaining_bytes"] += item["remaining_bytes"]
        elif status == "remaining":
            remaining_files += 1
            remaining_bytes += item["remaining_bytes"]
            bucket["needed_files"] += 1
            bucket["remaining_bytes"] += item["remaining_bytes"]
        elif status == "unavailable":
            unavailable_files += 1
            bucket["unavailable_files"] += 1
        else:
            unknown_files += 1
            bucket["unknown_files"] += 1

    free_bytes = int(shutil.disk_usage(root).free)
    speed_mbps, speed_source = _recent_speed_mbps(root)
    eta_seconds = None
    if speed_mbps > 0 and remaining_bytes > 0:
        eta_seconds = remaining_bytes * 8.0 / (speed_mbps * 1_000_000.0)

    return {
        "cancelled": bool(cancelled and cancelled()),
        "symbols": normalized_symbols,
        "planned_files": total,
        "resolved_files": len(results),
        "present_files": present_files,
        "partial_files": partial_files,
        "remaining_files": remaining_files + partial_files,
        "unavailable_files": unavailable_files,
        "unknown_files": unknown_files,
        "present_bytes": int(present_bytes),
        "partial_bytes": int(partial_bytes),
        "remaining_bytes": int(remaining_bytes),
        "free_bytes": free_bytes,
        "enough_disk": free_bytes >= remaining_bytes,
        "speed_mbps": speed_mbps,
        "speed_source": speed_source,
        "eta_seconds": eta_seconds,
        "by_dataset": by_dataset,
        "results": results,
    }
