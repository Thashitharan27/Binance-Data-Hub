"""Long-run archive collection with live connection-cap auto calibration."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from .archive_downloader import Manifest, _daily_fallback_tasks, plan_archive_tasks
from .fast_downloader import (
    DEFAULT_FILE_WORKERS,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SEGMENTS,
    DEFAULT_SEGMENT_THRESHOLD_MB,
    MAX_CONNECTIONS,
    MAX_FILE_WORKERS,
    MAX_SEGMENTS,
    _download_adaptive,
)
from .performance import TransferMeter, record_run_history
from .runtime_tuner import AdjustableConnectionGate, RuntimeAutoCalibrator


DEFAULT_RECALIBRATION_MINUTES = 60
DEFAULT_RECALIBRATION_PROBE_SECONDS = 20
DEFAULT_AUTO_MAX_CONNECTIONS = 32


def download_archive_library_runtime(
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
    auto_recalibrate: bool = False,
    recalibration_minutes: float = DEFAULT_RECALIBRATION_MINUTES,
    recalibration_probe_seconds: float = DEFAULT_RECALIBRATION_PROBE_SECONDS,
    auto_max_connections: int = DEFAULT_AUTO_MAX_CONNECTIONS,
    recalibration_event=None,
) -> dict:
    """Mirror requested archives and optionally optimize the cap during long runs."""
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
    gate = AdjustableConnectionGate(max_connections)

    def emit_telemetry(snapshot):
        if telemetry:
            enriched = dict(snapshot)
            enriched["connection_cap"] = gate.limit
            enriched["active_connections"] = gate.active
            enriched["auto_recalibrate"] = bool(auto_recalibrate)
            telemetry(enriched)

    meter = TransferMeter(callback=emit_telemetry)
    calibrator = RuntimeAutoCalibrator(
        gate,
        meter,
        enabled=auto_recalibrate,
        interval_seconds=max(1.0, float(recalibration_minutes)) * 60.0,
        probe_seconds=recalibration_probe_seconds,
        max_connections=max(max_connections, min(int(auto_max_connections), MAX_CONNECTIONS)),
        callback=recalibration_event,
        cancelled=cancelled,
    )

    def tracked_opener(request, **kwargs):
        try:
            return opener(request, **kwargs)
        except HTTPError as exc:
            if exc.code != 404:
                calibrator.report_error()
            raise
        except (URLError, OSError):
            calibrator.report_error()
            raise

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
    results = []
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
                    opener=tracked_opener,
                    gate=gate,
                    max_segments=segments,
                    segment_threshold_bytes=threshold_bytes,
                    on_bytes=meter.add_bytes,
                ): task
                for task in batch
            }
            for index, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result.status == "failed":
                    calibrator.report_error()
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

    calibrator.start()
    try:
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
    finally:
        calibrator.stop()

    statuses = ("downloaded", "skipped", "missing", "failed", "cancelled")
    counts = {status: sum(1 for item in results if item.status == status) for status in statuses}
    segmented_files = sum(
        1
        for item in results
        if item.status == "downloaded" and str(getattr(item, "transport", "")).startswith("segmented-")
    )
    total_processed_target = len(tasks) + len(fallbacks)
    performance = meter.finish(len(results), total_processed_target)
    performance["connection_cap"] = gate.limit
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
        "final_connection_cap": gate.limit,
        "segments": segments,
        "auto_recalibrate": bool(auto_recalibrate),
        "recalibration_minutes": float(recalibration_minutes),
        "connection_adjustments": list(calibrator.events),
        "results": results,
    }
    record_run_history(
        root,
        meter=meter,
        summary=summary,
        symbols=normalized_symbols,
        datasets=list(datasets),
        intervals=list(intervals),
        max_connections=gate.limit,
        segments=segments,
        verify=verify,
    )
    return summary
