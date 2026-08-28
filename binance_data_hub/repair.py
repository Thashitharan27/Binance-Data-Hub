"""On-demand kline coverage and integrity scanner with targeted repair tools.

This module is intentionally separate from normal archive collection. Nothing in
``download_archive_library`` calls it, so the Hub's speed-first download path
never pays the cost of opening CSVs or checking candle continuity/integrity.
"""
from __future__ import annotations

import csv
import io
import math
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import BoundedSemaphore

import pandas as pd

from .archive_downloader import ArchiveTask, Manifest, _month_end, _month_start, _next_month, _task
from .catalog import DATASETS
from .fast_downloader import _download_adaptive


KLINE_DATASETS = (
    "klines",
    "markPriceKlines",
    "indexPriceKlines",
    "premiumIndexKlines",
)

# Repair continuity is exact for fixed UTC candle intervals. Binance's 3d/1w/1mo
# anchors require exchange-calendar semantics, so those are intentionally rejected
# rather than reporting false gaps.
INTERVAL_MILLISECONDS = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}

DEFAULT_REPAIR_WORKERS = 16
MAX_INTEGRITY_DETAILS = 100


@dataclass(frozen=True)
class _ArchiveCandidate:
    path: Path
    period: str
    key: str
    span_start: date
    span_end: date


def _as_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if value in (None, ""):
        raise ValueError("Start and end dates are required for Data Repair.")
    return pd.Timestamp(value).date()


def _normalize_symbol(symbol: str) -> str:
    cleaned = str(symbol).strip().upper().replace("/", "")
    if not cleaned or not cleaned.isalnum():
        raise ValueError("Enter a valid USD-M symbol such as BTCUSDT.")
    return cleaned


def _validate_request(symbol: str, dataset: str, interval: str, start_date, end_date):
    symbol = _normalize_symbol(symbol)
    if dataset not in KLINE_DATASETS:
        raise ValueError(
            "Data Repair scanning currently supports kline datasets only: "
            + ", ".join(KLINE_DATASETS)
        )
    if interval not in INTERVAL_MILLISECONDS:
        raise ValueError(
            f"Interval {interval!r} is not supported by exact continuity repair. "
            "Use 1m through 1d fixed UTC intervals; 3d/1w/1mo are intentionally excluded."
        )
    start = _as_date(start_date)
    end = _as_date(end_date)
    if start > end:
        raise ValueError("Start date must be on or before end date.")
    return symbol, dataset, interval, start, end


def _monthly_path(root: Path, dataset: str, symbol: str, interval: str, month: date) -> Path:
    return (
        root
        / "raw"
        / "futures"
        / "um"
        / "monthly"
        / dataset
        / symbol
        / interval
        / f"{symbol}-{interval}-{month.strftime('%Y-%m')}.zip"
    )


def _daily_path(root: Path, dataset: str, symbol: str, interval: str, day: date) -> Path:
    return (
        root
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / dataset
        / symbol
        / interval
        / f"{symbol}-{interval}-{day.isoformat()}.zip"
    )


def _archive_candidates(root: Path, dataset: str, symbol: str, interval: str, start: date, end: date):
    candidates: list[_ArchiveCandidate] = []

    month = _month_start(start)
    while month <= end:
        month_last = _month_end(month)
        path = _monthly_path(root, dataset, symbol, interval, month)
        if path.exists():
            candidates.append(
                _ArchiveCandidate(path, "monthly", month.strftime("%Y-%m"), max(start, month), min(end, month_last))
            )
        month = _next_month(month)

    day = start
    while day <= end:
        path = _daily_path(root, dataset, symbol, interval, day)
        if path.exists():
            candidates.append(_ArchiveCandidate(path, "daily", day.isoformat(), day, day))
        day += timedelta(days=1)

    # Monthly is intentionally read before daily. The scanner stores the last
    # row seen for each timestamp, mirroring Strategy Lab's repair-source
    # precedence: a targeted daily archive can replace one bad/missing monthly
    # candle without modifying the official monthly ZIP.
    candidates.sort(key=lambda item: (item.span_start, 0 if item.period == "monthly" else 1, item.key))
    return candidates


def _normalize_epoch_ms(raw: str) -> int | None:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    # Binance is migrating some archive families from millisecond to microsecond
    # timestamps. Normalize either representation to milliseconds for coverage.
    if value >= 100_000_000_000_000:
        value //= 1000
    return value


def _number(row, index: int) -> float | None:
    if index >= len(row):
        return None
    raw = str(row[index]).strip()
    if raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return math.nan
    return value


def _kline_integrity_issues(row, dataset: str, timestamp: int, interval_ms: int) -> tuple[dict, ...]:
    """Return row-level issues that can make a present candle unsafe to consume.

    The checks intentionally mirror Strategy Lab's blocking kline invariants while
    staying tolerant of older/minimal archive schemas where optional Binance
    fields are absent. They do not synthesize or modify source values.
    """
    issues: list[dict] = []

    def add(code: str, message: str, **details):
        issues.append({"code": code, "message": message, "details": details})

    if timestamp % interval_ms != 0:
        add("OFF_GRID_TIMESTAMP", "Candle timestamp is off the expected UTC interval grid")

    if len(row) < 6:
        add("KLINE_SCHEMA_TOO_SHORT", "Kline row has fewer than the six required OHLCV columns")
        return tuple(issues)

    open_, high, low, close, volume = (_number(row, index) for index in range(1, 6))
    required = {"open": open_, "high": high, "low": low, "close": close, "volume": volume}
    for name, value in required.items():
        if value is None or not math.isfinite(value):
            add("INVALID_NUMERIC", f"{name} must be a finite number", field=name)

    if any(value is None or not math.isfinite(value) for value in (open_, high, low, close, volume)):
        return tuple(issues)

    if dataset != "premiumIndexKlines":
        for name, value in (("open", open_), ("high", high), ("low", low), ("close", close)):
            if value <= 0:
                add("INVALID_DOMAIN_VALUE", f"{name} must be positive", field=name, value=value)
    if volume < 0:
        add("INVALID_DOMAIN_VALUE", "volume must be non-negative", field="volume", value=volume)

    if low > high or open_ < low or open_ > high or close < low or close > high:
        add("INVALID_OHLC", "OHLC values violate candle bounds")

    optional_names = {
        7: "quote_volume",
        8: "trade_count",
        9: "taker_buy_base_volume",
        10: "taker_buy_quote_volume",
    }
    optional: dict[str, float | None] = {}
    for index, name in optional_names.items():
        value = _number(row, index)
        optional[name] = value
        if value is None:
            continue
        if not math.isfinite(value):
            add("INVALID_NUMERIC", f"{name} must be finite when present", field=name)
        elif value < 0:
            add("INVALID_DOMAIN_VALUE", f"{name} must be non-negative", field=name, value=value)

    taker_base = optional.get("taker_buy_base_volume")
    if taker_base is not None and math.isfinite(taker_base):
        tolerance = max(abs(volume) * 1e-12, 1e-12)
        if taker_base > volume + tolerance:
            add(
                "TAKER_VOLUME_EXCEEDS_TOTAL",
                "taker_buy_base_volume exceeds volume",
                total=volume,
                taker=taker_base,
            )

    quote_volume = optional.get("quote_volume")
    taker_quote = optional.get("taker_buy_quote_volume")
    if (
        quote_volume is not None
        and taker_quote is not None
        and math.isfinite(quote_volume)
        and math.isfinite(taker_quote)
    ):
        tolerance = max(abs(quote_volume) * 1e-12, 1e-12)
        if taker_quote > quote_volume + tolerance:
            add(
                "TAKER_QUOTE_VOLUME_EXCEEDS_TOTAL",
                "taker_buy_quote_volume exceeds quote_volume",
                total=quote_volume,
                taker=taker_quote,
            )

    return tuple(issues)


def _read_archive_candles(
    candidate: _ArchiveCandidate,
    dataset: str,
    interval_ms: int,
    range_start_ms: int,
    range_end_ms: int,
):
    timestamps: set[int] = set()
    candle_issues: dict[int, tuple[dict, ...]] = {}
    duplicates = 0
    rows = 0
    try:
        with zipfile.ZipFile(candidate.path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_members:
                return timestamps, candle_issues, duplicates, rows, "ZIP contains no CSV file", True
            for name in csv_members:
                with archive.open(name) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    reader = csv.reader(text)
                    local_seen: set[int] = set()
                    for row in reader:
                        if not row:
                            continue
                        timestamp = _normalize_epoch_ms(row[0])
                        if timestamp is None:
                            # Header rows are allowed; a non-numeric first column
                            # elsewhere cannot be placed on the timeline and is ignored.
                            continue
                        rows += 1
                        if range_start_ms <= timestamp <= range_end_ms:
                            if timestamp in local_seen:
                                duplicates += 1
                            local_seen.add(timestamp)
                            timestamps.add(timestamp)
                            # Last row wins inside an archive, matching the canonical
                            # kline adapter's duplicate resolution.
                            candle_issues[timestamp] = _kline_integrity_issues(
                                row, dataset, timestamp, interval_ms
                            )
        if rows == 0:
            return timestamps, candle_issues, duplicates, rows, "CSV contains no timestamp rows", True
        return timestamps, candle_issues, duplicates, rows, None, True
    except (OSError, UnicodeError, csv.Error, zipfile.BadZipFile, RuntimeError) as exc:
        return timestamps, candle_issues, duplicates, rows, str(exc), zipfile.is_zipfile(candidate.path)


def _day_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date().isoformat()


def _iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def scan_kline_range(
    root: str | Path,
    symbol: str,
    dataset: str,
    interval: str,
    start_date,
    end_date,
    *,
    progress=None,
    cancelled=None,
) -> dict:
    """Scan a local kline range for both continuity and row integrity.

    Monthly and daily rows are combined logically. Daily rows take precedence for
    the same timestamp, so a targeted daily repair can replace a bad monthly row
    while the official monthly ZIP remains byte-for-byte untouched.
    """
    symbol, dataset, interval, start, end = _validate_request(
        symbol, dataset, interval, start_date, end_date
    )
    root = Path(root).resolve()
    interval_ms = INTERVAL_MILLISECONDS[interval]
    range_start_ms = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    range_end_exclusive_ms = int(
        datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000
    )
    range_end_ms = range_end_exclusive_ms - 1

    candidates = _archive_candidates(root, dataset, symbol, interval, start, end)
    logical_timestamps: set[int] = set()
    effective_integrity: dict[int, dict] = {}
    archive_duplicates = 0
    invalid_archives = []
    archives = []

    for index, candidate in enumerate(candidates, 1):
        if cancelled and cancelled():
            return {
                "cancelled": True,
                "symbol": symbol,
                "dataset": dataset,
                "interval": interval,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
            }
        timestamps, candle_issues, duplicates, rows, error, valid_zip = _read_archive_candles(
            candidate, dataset, interval_ms, range_start_ms, range_end_ms
        )
        logical_timestamps.update(timestamps)
        for timestamp in timestamps:
            effective_integrity[timestamp] = {
                "issues": candle_issues.get(timestamp, ()),
                "source_period": candidate.period,
                "source_key": candidate.key,
                "source_path": str(candidate.path),
            }
        archive_duplicates += duplicates
        invalid_in_archive = sum(1 for issues in candle_issues.values() if issues)
        archive_info = {
            "path": str(candidate.path),
            "period": candidate.period,
            "key": candidate.key,
            "rows": rows,
            "candles_in_range": len(timestamps),
            "invalid_candles": invalid_in_archive,
            "duplicates": duplicates,
            "error": error,
            "valid_zip": valid_zip,
        }
        archives.append(archive_info)
        if error:
            invalid_archives.append(archive_info)
        if progress:
            progress(index, len(candidates), archive_info)

    expected_count = 0
    missing_count = 0
    missing_by_day: dict[str, int] = defaultdict(int)
    timestamp = range_start_ms
    # Requested dates are UTC-day boundaries, which align exactly with all fixed
    # intervals accepted above.
    while timestamp < range_end_exclusive_ms:
        expected_count += 1
        if timestamp not in logical_timestamps:
            missing_count += 1
            missing_by_day[_day_from_ms(timestamp)] += 1
        timestamp += interval_ms

    invalid_by_day: dict[str, int] = defaultdict(int)
    issue_counts: Counter[str] = Counter()
    integrity_issues = []
    invalid_candle_count = 0
    for timestamp, state in sorted(effective_integrity.items()):
        issues = tuple(state.get("issues") or ())
        if not issues:
            continue
        invalid_candle_count += 1
        day = _day_from_ms(timestamp)
        invalid_by_day[day] += 1
        for issue in issues:
            issue_counts[str(issue.get("code", "UNKNOWN"))] += 1
        if len(integrity_issues) < MAX_INTEGRITY_DETAILS:
            integrity_issues.append(
                {
                    "timestamp": _iso_from_ms(timestamp),
                    "day": day,
                    "codes": [issue.get("code", "UNKNOWN") for issue in issues],
                    "issues": list(issues),
                    "source_period": state.get("source_period"),
                    "source_key": state.get("source_key"),
                    "source_path": state.get("source_path"),
                }
            )

    found_expected = expected_count - missing_count
    missing_by_day = dict(sorted(missing_by_day.items()))
    invalid_by_day = dict(sorted(invalid_by_day.items()))
    return {
        "cancelled": False,
        "symbol": symbol,
        "dataset": dataset,
        "interval": interval,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "expected_candles": expected_count,
        "found_candles": found_expected,
        "missing_candles": missing_count,
        "missing_days": len(missing_by_day),
        "missing_by_day": missing_by_day,
        "invalid_candles": invalid_candle_count,
        "invalid_days": len(invalid_by_day),
        "invalid_by_day": invalid_by_day,
        "integrity_issue_counts": dict(sorted(issue_counts.items())),
        "integrity_issues": integrity_issues,
        "integrity_details_truncated": invalid_candle_count > len(integrity_issues),
        "archive_duplicates": archive_duplicates,
        "archives_scanned": len(candidates),
        "invalid_archives": invalid_archives,
        "archives": archives,
        "complete": (
            missing_count == 0
            and invalid_candle_count == 0
            and not invalid_archives
        ),
    }


def _monthly_repair_tasks(root: Path, scan: dict, today: date) -> list[ArchiveTask]:
    """Prefer one monthly download when an entire local monthly source is absent/corrupt.

    A valid existing monthly ZIP is never re-downloaded merely because its CSV has
    internal candle gaps or a bad candle value; those are repaired with daily
    archives instead.
    """
    dataset = scan["dataset"]
    symbol = scan["symbol"]
    interval = scan["interval"]
    spec = DATASETS[dataset]
    if not spec.monthly:
        return []

    start = _as_date(scan["start_date"])
    end = _as_date(scan["end_date"])
    current_month = _month_start(today)
    invalid_zip_paths = {
        Path(item["path"]).resolve()
        for item in scan.get("invalid_archives", [])
        if not item.get("valid_zip")
    }
    missing_days = set(scan.get("missing_by_day", {}))
    tasks = []

    month = _month_start(start)
    while month < current_month and month <= end:
        month_last = _month_end(month)
        overlap_start = max(start, month)
        overlap_end = min(end, month_last)
        month_missing = False
        day = overlap_start
        while day <= overlap_end:
            if day.isoformat() in missing_days:
                month_missing = True
                break
            day += timedelta(days=1)
        if month_missing:
            path = _monthly_path(root, dataset, symbol, interval, month)
            if not path.exists() or path.resolve() in invalid_zip_paths:
                tasks.append(
                    _task(
                        spec,
                        symbol,
                        "monthly",
                        month.strftime("%Y-%m"),
                        interval,
                        overlap_start,
                        overlap_end,
                    )
                )
        month = _next_month(month)
    return tasks


def _daily_repair_tasks(scan: dict) -> list[ArchiveTask]:
    spec = DATASETS[scan["dataset"]]
    if not spec.daily:
        return []
    repair_days = sorted(
        set(scan.get("missing_by_day", {}))
        | set(scan.get("invalid_by_day", {}))
    )
    tasks = []
    for day_text in repair_days:
        day = date.fromisoformat(day_text)
        tasks.append(
            _task(spec, scan["symbol"], "daily", day_text, scan["interval"], day, day)
        )
    return tasks


def _run_repair_tasks(
    tasks: list[ArchiveTask],
    root: Path,
    *,
    verify: bool,
    max_connections: int,
    progress=None,
    cancelled=None,
    opener=None,
):
    if not tasks:
        return []
    if opener is None:
        from urllib.request import urlopen
        opener = urlopen

    gate = BoundedSemaphore(max(1, int(max_connections)))
    manifest = Manifest(root / "manifest.sqlite")
    results = []
    worker_count = min(DEFAULT_REPAIR_WORKERS, max(1, int(max_connections)), len(tasks))
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                _download_adaptive,
                task,
                root,
                verify=verify,
                cancelled=cancelled,
                opener=opener,
                gate=gate,
                max_segments=1,
                segment_threshold_bytes=24 * 1024 * 1024,
            ): task
            for task in tasks
        }
        for index, future in enumerate(as_completed(futures), 1):
            result = future.result()
            manifest.record(result)
            results.append(result)
            if progress:
                progress(index, len(tasks), result)
            if cancelled and cancelled():
                for pending in futures:
                    pending.cancel()
                break
    return results


def scan_and_repair_kline_range(
    root: str | Path,
    symbol: str,
    dataset: str,
    interval: str,
    start_date,
    end_date,
    *,
    verify: bool = False,
    max_connections: int = 16,
    progress=None,
    cancelled=None,
    opener=None,
) -> dict:
    """Scan a requested range, repair missing/invalid candles, then verify again.

    Repair order is deliberately conservative:
    1. Missing/corrupt local monthly ZIP -> fetch that monthly ZIP once.
    2. Re-scan.
    3. Valid monthly ZIP with missing or invalid candles -> fetch only the affected
       Binance daily ZIPs. Daily rows logically override monthly rows.
    4. Re-scan and report unresolved/upstream source problems.
    """
    root = Path(root).resolve()
    symbol, dataset, interval, start, end = _validate_request(
        symbol, dataset, interval, start_date, end_date
    )

    def emit(stage, done, total, detail):
        if progress:
            progress(stage, done, total, detail)

    before = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start,
        end,
        progress=lambda done, total, info: emit("scan-before", done, total, info),
        cancelled=cancelled,
    )
    if before.get("cancelled") or before.get("complete"):
        return {"cancelled": before.get("cancelled", False), "before": before, "after": before, "repair_results": []}

    today = datetime.now(timezone.utc).date()
    repair_results = []

    monthly_tasks = _monthly_repair_tasks(root, before, today)
    monthly_results = _run_repair_tasks(
        monthly_tasks,
        root,
        verify=verify,
        max_connections=max_connections,
        progress=lambda done, total, result: emit("repair-monthly", done, total, result),
        cancelled=cancelled,
        opener=opener,
    )
    repair_results.extend(monthly_results)

    middle = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start,
        end,
        progress=lambda done, total, info: emit("scan-middle", done, total, info),
        cancelled=cancelled,
    )
    if middle.get("cancelled"):
        return {"cancelled": True, "before": before, "after": middle, "repair_results": repair_results}

    daily_tasks = _daily_repair_tasks(middle)
    daily_results = _run_repair_tasks(
        daily_tasks,
        root,
        verify=verify,
        max_connections=max_connections,
        progress=lambda done, total, result: emit("repair-daily", done, total, result),
        cancelled=cancelled,
        opener=opener,
    )
    repair_results.extend(daily_results)

    after = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start,
        end,
        progress=lambda done, total, info: emit("scan-after", done, total, info),
        cancelled=cancelled,
    )

    source_missing_days = sorted(
        {
            result.task.span_start.isoformat()
            for result in daily_results
            if result.status == "missing"
        }
    )
    failed_days = sorted(
        {
            result.task.span_start.isoformat()
            for result in daily_results
            if result.status in {"failed", "cancelled"}
        }
    )
    unresolved_days = sorted(
        set(after.get("missing_by_day", {}))
        | set(after.get("invalid_by_day", {}))
    )

    return {
        "cancelled": after.get("cancelled", False),
        "before": before,
        "after": after,
        "repair_results": repair_results,
        "monthly_repairs": len(monthly_results),
        "daily_repairs": len(daily_results),
        "source_missing_days": source_missing_days,
        "failed_days": failed_days,
        "unresolved_days": unresolved_days,
    }
