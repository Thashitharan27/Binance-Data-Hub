"""Adaptive high-throughput transport for Binance public archive files.

Small archives use one resumable stream. Large monthly archives are probed with
HEAD and, when byte ranges are supported, split across several HTTP range
requests. A global semaphore caps total live HTTP connections across all files.
"""
from __future__ import annotations

import math
import os
import shutil
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import BoundedSemaphore
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .archive_downloader import (
    USER_AGENT,
    ArchiveTask,
    DownloadResult,
    Manifest,
    _daily_fallback_tasks,
    _fetch_checksum,
    _sha256,
    plan_archive_tasks,
)
from .performance import TransferMeter, record_run_history

CHUNK_SIZE = 1024 * 1024
DEFAULT_FILE_WORKERS = 32
MAX_FILE_WORKERS = 64
DEFAULT_MAX_CONNECTIONS = 32
MAX_CONNECTIONS = 128
DEFAULT_SEGMENTS = 4
MAX_SEGMENTS = 8
DEFAULT_SEGMENT_THRESHOLD_MB = 24
TARGET_SEGMENT_BYTES = 64 * 1024 * 1024

_SEGMENT_CANDIDATE_DATASETS = {
    "klines",
    "markPriceKlines",
    "indexPriceKlines",
    "premiumIndexKlines",
    "aggTrades",
    "trades",
    "bookDepth",
    "bookTicker",
}


class _RangeUnsupported(RuntimeError):
    pass


def _network_call(gate: BoundedSemaphore):
    class _Permit:
        def __enter__(self):
            gate.acquire()
            return self

        def __exit__(self, *_):
            gate.release()
            return False

    return _Permit()


def _eligible_for_segmentation(task: ArchiveTask) -> bool:
    if task.period != "monthly" or task.dataset not in _SEGMENT_CANDIDATE_DATASETS:
        return False
    if task.dataset in {"aggTrades", "trades", "bookDepth", "bookTicker"}:
        return True
    return task.interval in {"1m", "3m", "5m"}


def _probe_range_support(task: ArchiveTask, opener, gate: BoundedSemaphore) -> tuple[int | None, bool]:
    """Return (content_length, accepts_ranges) without downloading the body."""
    request = Request(task.url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with _network_call(gate):
            with opener(request, timeout=30) as response:
                headers = getattr(response, "headers", {}) or {}
                raw_length = headers.get("Content-Length") or headers.get("content-length")
                accept_ranges = str(headers.get("Accept-Ranges") or headers.get("accept-ranges") or "").lower()
                try:
                    length = int(raw_length) if raw_length is not None else None
                except (TypeError, ValueError):
                    length = None
                return length, "bytes" in accept_ranges
    except (HTTPError, URLError, OSError):
        return None, False


def _retry_sleep(exc, attempt: int):
    if isinstance(exc, HTTPError):
        retry_after = getattr(exc, "headers", {}).get("Retry-After") if getattr(exc, "headers", None) else None
        if retry_after:
            try:
                return float(retry_after)
            except (TypeError, ValueError):
                pass
    return min(2**attempt, 10)


def _download_single(task: ArchiveTask, final: Path, opener, gate: BoundedSemaphore, cancelled, on_bytes=None, retries: int = 5) -> DownloadResult:
    part = final.with_name(f"{final.name}.part")

    for attempt in range(retries):
        if cancelled and cancelled():
            result = DownloadResult(task, "cancelled", part.stat().st_size if part.exists() else 0)
            result.transport = "single"
            return result

        resume_from = part.stat().st_size if part.exists() else 0
        headers = {"User-Agent": USER_AGENT}
        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"
        request = Request(task.url, headers=headers)

        try:
            with _network_call(gate):
                with opener(request, timeout=120) as response:
                    status = getattr(response, "status", None)
                    if status is None and hasattr(response, "getcode"):
                        status = response.getcode()
                    append = bool(resume_from and status == 206)
                    mode = "ab" if append else "wb"
                    with part.open(mode) as output:
                        while True:
                            if cancelled and cancelled():
                                result = DownloadResult(task, "cancelled", part.stat().st_size if part.exists() else 0)
                                result.transport = "single"
                                return result
                            block = response.read(CHUNK_SIZE)
                            if not block:
                                break
                            output.write(block)
                            if on_bytes:
                                on_bytes(len(block))

            if not part.exists() or not part.stat().st_size:
                raise RuntimeError("Binance returned an empty archive.")
            result = DownloadResult(task, "downloaded", part.stat().st_size)
            result.transport = "single"
            return result

        except HTTPError as exc:
            if exc.code == 404:
                part.unlink(missing_ok=True)
                result = DownloadResult(task, "missing", error="HTTP 404")
                result.transport = "single"
                return result
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                result = DownloadResult(task, "failed", part.stat().st_size if part.exists() else 0, error=f"HTTP {exc.code}")
                result.transport = "single"
                return result
            time.sleep(_retry_sleep(exc, attempt))
        except (URLError, OSError, RuntimeError) as exc:
            if attempt == retries - 1:
                result = DownloadResult(task, "failed", part.stat().st_size if part.exists() else 0, error=str(exc))
                result.transport = "single"
                return result
            time.sleep(_retry_sleep(exc, attempt))

    result = DownloadResult(task, "failed", error="retry limit reached")
    result.transport = "single"
    return result


def _segment_ranges(total_size: int, max_segments: int) -> list[tuple[int, int]]:
    wanted = max(2, math.ceil(total_size / TARGET_SEGMENT_BYTES))
    count = max(2, min(max_segments, wanted))
    segment_size = math.ceil(total_size / count)
    ranges = []
    for index in range(count):
        start = index * segment_size
        if start >= total_size:
            break
        end = min(total_size - 1, start + segment_size - 1)
        ranges.append((start, end))
    return ranges


def _download_range(task: ArchiveTask, segment_path: Path, start: int, end: int, opener, gate: BoundedSemaphore, cancelled, on_bytes=None, retries: int = 5) -> None:
    expected_size = end - start + 1
    if segment_path.exists() and segment_path.stat().st_size == expected_size:
        return
    if segment_path.exists() and segment_path.stat().st_size > expected_size:
        segment_path.unlink()

    for attempt in range(retries):
        if cancelled and cancelled():
            raise InterruptedError("cancelled")

        existing = segment_path.stat().st_size if segment_path.exists() else 0
        range_start = start + existing
        if range_start > end:
            return

        request = Request(task.url, headers={"User-Agent": USER_AGENT, "Range": f"bytes={range_start}-{end}"})
        try:
            with _network_call(gate):
                with opener(request, timeout=120) as response:
                    status = getattr(response, "status", None)
                    if status is None and hasattr(response, "getcode"):
                        status = response.getcode()
                    if status != 206:
                        raise _RangeUnsupported(f"server returned HTTP {status} to Range request")
                    mode = "ab" if existing else "wb"
                    with segment_path.open(mode) as output:
                        while True:
                            if cancelled and cancelled():
                                raise InterruptedError("cancelled")
                            block = response.read(CHUNK_SIZE)
                            if not block:
                                break
                            output.write(block)
                            if on_bytes:
                                on_bytes(len(block))

            if segment_path.stat().st_size != expected_size:
                raise RuntimeError(f"incomplete byte range {start}-{end}: {segment_path.stat().st_size} of {expected_size} bytes")
            return
        except _RangeUnsupported:
            raise
        except HTTPError as exc:
            if exc.code == 404:
                raise
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
            time.sleep(_retry_sleep(exc, attempt))
        except (URLError, OSError, RuntimeError) as exc:
            if attempt == retries - 1:
                raise
            time.sleep(_retry_sleep(exc, attempt))


def _download_segmented(task: ArchiveTask, final: Path, total_size: int, max_segments: int, opener, gate: BoundedSemaphore, cancelled, on_bytes=None) -> DownloadResult:
    ranges = _segment_ranges(total_size, max_segments)
    segment_dir = final.with_name(f".{final.name}.segments")
    segment_dir.mkdir(parents=True, exist_ok=True)

    try:
        with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
            futures = []
            for index, (start, end) in enumerate(ranges):
                segment_path = segment_dir / f"{index:02d}-{start}-{end}.part"
                futures.append(pool.submit(_download_range, task, segment_path, start, end, opener, gate, cancelled, on_bytes))
            for future in as_completed(futures):
                future.result()

        if cancelled and cancelled():
            result = DownloadResult(task, "cancelled")
            result.transport = f"segmented-{len(ranges)}"
            return result

        combined = final.with_name(f"{final.name}.part")
        with combined.open("wb") as output:
            for index, (start, end) in enumerate(ranges):
                segment_path = segment_dir / f"{index:02d}-{start}-{end}.part"
                with segment_path.open("rb") as source:
                    shutil.copyfileobj(source, output, length=CHUNK_SIZE)

        actual_size = combined.stat().st_size
        if actual_size != total_size:
            raise RuntimeError(f"combined archive size mismatch: {actual_size} != {total_size}")
        result = DownloadResult(task, "downloaded", actual_size)
        result.transport = f"segmented-{len(ranges)}"
        return result

    except InterruptedError:
        result = DownloadResult(task, "cancelled")
        result.transport = f"segmented-{len(ranges)}"
        return result
    except HTTPError as exc:
        if exc.code == 404:
            shutil.rmtree(segment_dir, ignore_errors=True)
            result = DownloadResult(task, "missing", error="HTTP 404")
            result.transport = f"segmented-{len(ranges)}"
            return result
        result = DownloadResult(task, "failed", error=f"HTTP {exc.code}")
        result.transport = f"segmented-{len(ranges)}"
        return result
    except _RangeUnsupported as exc:
        result = DownloadResult(task, "range-unsupported", error=str(exc))
        result.transport = f"segmented-{len(ranges)}"
        return result
    except (URLError, OSError, RuntimeError) as exc:
        result = DownloadResult(task, "failed", error=str(exc))
        result.transport = f"segmented-{len(ranges)}"
        return result


def _finalize_download(result: DownloadResult, final: Path, task: ArchiveTask, verify: bool, opener) -> DownloadResult:
    if result.status != "downloaded":
        return result

    part = final.with_name(f"{final.name}.part")
    if not part.exists() or not part.stat().st_size:
        result.status = "failed"
        result.error = "download completed without a part file"
        return result
    if not zipfile.is_zipfile(part):
        part.unlink(missing_ok=True)
        result.status = "failed"
        result.error = "downloaded file is not a valid ZIP archive"
        return result

    if verify:
        expected = _fetch_checksum(task, opener)
        if expected is None:
            result.status = "failed"
            result.error = "checksum file is missing"
            return result
        digest = _sha256(part)
        if digest != expected:
            part.unlink(missing_ok=True)
            result.status = "failed"
            result.error = f"SHA-256 mismatch: expected {expected}, got {digest}"
            return result
        result.sha256 = digest

    os.replace(part, final)
    shutil.rmtree(final.with_name(f".{final.name}.segments"), ignore_errors=True)
    result.bytes = final.stat().st_size
    return result


def _download_adaptive(task: ArchiveTask, root: Path, *, verify: bool, cancelled, opener, gate: BoundedSemaphore, max_segments: int, segment_threshold_bytes: int, on_bytes=None) -> DownloadResult:
    final = root / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)

    if final.exists() and final.stat().st_size > 0:
        if not verify:
            result = DownloadResult(task, "skipped", final.stat().st_size)
            result.transport = "existing"
            return result
        expected = _fetch_checksum(task, opener)
        if expected and _sha256(final) == expected:
            result = DownloadResult(task, "skipped", final.stat().st_size, expected)
            result.transport = "existing"
            return result

    if _eligible_for_segmentation(task) and max_segments > 1:
        total_size, accepts_ranges = _probe_range_support(task, opener, gate)
        if total_size and accepts_ranges and total_size >= segment_threshold_bytes:
            segmented = _download_segmented(task, final, total_size, max_segments, opener, gate, cancelled, on_bytes)
            if segmented.status == "downloaded":
                return _finalize_download(segmented, final, task, verify, opener)
            if segmented.status not in {"range-unsupported"}:
                return segmented
            shutil.rmtree(final.with_name(f".{final.name}.segments"), ignore_errors=True)

    single = _download_single(task, final, opener, gate, cancelled, on_bytes)
    return _finalize_download(single, final, task, verify, opener)


def download_archive_library(
    symbols,
    datasets,
    intervals,
    root,
    start_date=None,
    end_date=None,
    *,
    workers: int = DEFAULT_FILE_WORKERS,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    segments: int = DEFAULT_SEGMENTS,
    segment_threshold_mb: float = DEFAULT_SEGMENT_THRESHOLD_MB,
    verify: bool = False,
    progress=None,
    telemetry=None,
    cancelled=None,
    opener=urlopen,
    today=None,
) -> dict:
    """Mirror requested archives using adaptive single/segmented transports."""
    workers = int(workers)
    max_connections = int(max_connections)
    segments = int(segments)
    threshold_bytes = max(1, int(float(segment_threshold_mb) * 1024 * 1024))

    if not 1 <= workers <= MAX_FILE_WORKERS:
        raise ValueError(f"workers must be between 1 and {MAX_FILE_WORKERS}")
    if not 1 <= max_connections <= MAX_CONNECTIONS:
        raise ValueError(f"max_connections must be between 1 and {MAX_CONNECTIONS}")
    if not 1 <= segments <= MAX_SEGMENTS:
        raise ValueError(f"segments must be between 1 and {MAX_SEGMENTS}")

    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(root / "manifest.sqlite")
    gate = BoundedSemaphore(max_connections)
    meter = TransferMeter(callback=telemetry)

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
    meter.mark_files(0, len(tasks))

    def run_batch(batch, completed_base, total_hint):
        batch_results = []
        if not batch:
            return batch_results
        with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as pool:
            futures = {
                pool.submit(
                    _download_adaptive,
                    task,
                    root,
                    verify=verify,
                    cancelled=cancelled,
                    opener=opener,
                    gate=gate,
                    max_segments=segments,
                    segment_threshold_bytes=threshold_bytes,
                    on_bytes=meter.add_bytes,
                ): task
                for task in batch
            }
            for index, future in enumerate(as_completed(futures), 1):
                result = future.result()
                manifest.record(result)
                batch_results.append(result)
                completed = completed_base + index
                meter.mark_files(completed, total_hint)
                if progress:
                    progress(completed, total_hint, result)
                if cancelled and cancelled():
                    for pending in futures:
                        pending.cancel()
                    break
        return batch_results

    primary = run_batch(tasks, 0, len(tasks))
    results.extend(primary)
    fallbacks = []
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
        total_with_fallbacks = len(tasks) + len(fallbacks)
        meter.mark_files(len(primary), total_with_fallbacks)
        results.extend(run_batch(fallbacks, len(primary), total_with_fallbacks))

    statuses = ("downloaded", "skipped", "missing", "failed", "cancelled")
    counts = {status: sum(1 for item in results if item.status == status) for status in statuses}
    segmented_files = sum(
        1
        for item in results
        if item.status == "downloaded" and str(getattr(item, "transport", "")).startswith("segmented-")
    )
    total_processed_target = len(tasks) + len(fallbacks)
    performance = meter.finish(len(results), total_processed_target)
    summary = {
        "root": str(root),
        "planned": len(tasks),
        "fallbacks": len(fallbacks),
        "files": len(results),
        "bytes_downloaded": sum(item.bytes for item in results if item.status == "downloaded"),
        "counts": counts,
        "segmented_files": segmented_files,
        "performance": performance,
        "max_connections": max_connections,
        "segments": segments,
        "results": results,
    }
    record_run_history(
        root,
        meter=meter,
        summary=summary,
        symbols=normalized_symbols,
        datasets=list(datasets),
        intervals=list(intervals),
        max_connections=max_connections,
        segments=segments,
        verify=verify,
    )
    return summary
