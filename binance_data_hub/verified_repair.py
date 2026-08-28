"""Verified kline repair fallback reconstructed from Binance daily aggTrades.

The ordinary Data Repair path remains authoritative for missing/corrupt archives
and for daily-kline replacement. This module adds one deliberately narrow final
fallback for contract klines whose daily Binance kline is still internally
invalid. It downloads only the affected UTC day's aggTrades archive,
reconstructs the candle, and writes a provenance-rich overlay without modifying
any official Binance ZIP.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .archive_downloader import _task
from .catalog import DATASETS
from .repair import (
    INTERVAL_MILLISECONDS,
    KLINE_DATASETS,
    _as_date,
    _kline_integrity_issues,
    _normalize_epoch_ms,
    _run_repair_tasks,
    scan_and_repair_kline_range as _base_scan_and_repair_kline_range,
    scan_kline_range as _base_scan_kline_range,
)


# Only volume-family inconsistencies are auto-reconstructed. A broken price
# path or timestamp remains blocking rather than being silently rewritten.
_AGGTRADES_REPAIRABLE_CODES = {
    "TAKER_VOLUME_EXCEEDS_TOTAL",
    "TAKER_QUOTE_VOLUME_EXCEEDS_TOTAL",
}
_VOLUME_FIELD_INDEXES = {
    "volume": 5,
    "quote_volume": 7,
    "taker_buy_base_volume": 9,
    "taker_buy_quote_volume": 10,
}


def _overlay_path(root: Path, symbol: str, interval: str, day: date) -> Path:
    """Return a discoverable kline overlay path with explicit last precedence.

    Strategy Lab recursively discovers kline archives and orders same-period
    sources by path. Placing the verified layer under ``zz_verified_repairs``
    keeps official monthly/daily ZIPs untouched while making this overlay the
    final source for only the reconstructed timestamps.
    """
    return (
        root
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / "klines"
        / symbol
        / interval
        / "zz_verified_repairs"
        / f"{symbol}-{interval}-{day.isoformat()}.verified-repair.zip"
    )


def _aggtrades_path(root: Path, symbol: str, day: date) -> Path:
    return (
        root
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / "aggTrades"
        / symbol
        / f"{symbol}-aggTrades-{day.isoformat()}.zip"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _zip_csv_rows(path: Path):
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"ZIP contains no CSV file: {path}")
        member = max(members, key=lambda name: archive.getinfo(name).file_size)
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            yield from csv.reader(text)


def _read_kline_rows(path: Path, timestamps: set[int]) -> dict[int, list[str]]:
    result: dict[int, list[str]] = {}
    if not path.is_file() or not timestamps:
        return result
    for row in _zip_csv_rows(path):
        if not row:
            continue
        timestamp = _normalize_epoch_ms(row[0])
        if timestamp in timestamps:
            result[timestamp] = [str(value).strip() for value in row]
    return result


def _read_overlay(path: Path):
    rows: dict[int, list[str]] = {}
    manifest = {
        "version": 1,
        "method": "BINANCE_DAILY_AGGTRADES_RECONSTRUCTION",
        "repairs": [],
    }
    if not path.is_file():
        return rows, manifest
    try:
        with zipfile.ZipFile(path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if csv_members:
                member = max(csv_members, key=lambda name: archive.getinfo(name).file_size)
                with archive.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    for row in csv.reader(text):
                        if not row:
                            continue
                        timestamp = _normalize_epoch_ms(row[0])
                        if timestamp is not None:
                            rows[timestamp] = [str(value).strip() for value in row]
            if "repair_manifest.json" in archive.namelist():
                manifest = json.loads(archive.read("repair_manifest.json").decode("utf-8"))
    except (OSError, ValueError, json.JSONDecodeError, zipfile.BadZipFile):
        return {}, {
            "version": 1,
            "method": "BINANCE_DAILY_AGGTRADES_RECONSTRUCTION",
            "repairs": [],
        }
    return rows, manifest


def _manifest_codes_by_timestamp(manifest: dict) -> dict[int, list[str]]:
    result: dict[int, list[str]] = {}
    for repair in manifest.get("repairs", []):
        try:
            timestamp = int(repair["timestamp_ms"])
        except (KeyError, TypeError, ValueError):
            continue
        result[timestamp] = [str(code) for code in repair.get("original_issue_codes", [])]
    return result


def _apply_verified_overlays(root: Path, scan: dict) -> dict:
    """Overlay verified rows onto a base scan result for Data Repair presentation."""
    if scan.get("cancelled") or scan.get("dataset") != "klines":
        return scan

    start = _as_date(scan["start_date"])
    end = _as_date(scan["end_date"])
    symbol = scan["symbol"]
    interval = scan["interval"]
    interval_ms = INTERVAL_MILLISECONDS[interval]
    range_start_ms = int(
        datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000
    )
    range_end_ms = int(
        datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc).timestamp() * 1000
    )

    result = dict(scan)
    missing_by_day = dict(scan.get("missing_by_day", {}))
    invalid_by_day = dict(scan.get("invalid_by_day", {}))
    issue_counts = Counter(scan.get("integrity_issue_counts", {}))
    integrity_details = list(scan.get("integrity_issues", []))
    detail_by_timestamp = {str(item.get("timestamp")): item for item in integrity_details}
    verified_repairs = []
    overlays = []

    day = start
    while day <= end:
        path = _overlay_path(root, symbol, interval, day)
        day = date.fromordinal(day.toordinal() + 1)
        if not path.is_file():
            continue
        rows, manifest = _read_overlay(path)
        codes_by_timestamp = _manifest_codes_by_timestamp(manifest)
        valid_rows = {}
        for timestamp, row in rows.items():
            if not (range_start_ms <= timestamp <= range_end_ms):
                continue
            if _kline_integrity_issues(row, "klines", timestamp, interval_ms):
                continue
            valid_rows[timestamp] = row
        if not valid_rows:
            continue

        overlays.append(
            {
                "path": str(path),
                "period": "verified_repair",
                "key": path.name,
                "rows": len(rows),
                "candles_in_range": len(valid_rows),
                "invalid_candles": 0,
                "duplicates": 0,
                "error": None,
                "valid_zip": True,
            }
        )
        for timestamp in sorted(valid_rows):
            iso = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat()
            day_text = iso[:10]
            was_missing = int(missing_by_day.get(day_text, 0)) > 0
            detail = detail_by_timestamp.get(iso)
            was_invalid = detail is not None or int(invalid_by_day.get(day_text, 0)) > 0

            if was_missing:
                remaining = int(missing_by_day.get(day_text, 0)) - 1
                if remaining > 0:
                    missing_by_day[day_text] = remaining
                else:
                    missing_by_day.pop(day_text, None)
            if was_invalid:
                remaining = int(invalid_by_day.get(day_text, 0)) - 1
                if remaining > 0:
                    invalid_by_day[day_text] = remaining
                else:
                    invalid_by_day.pop(day_text, None)
                codes = codes_by_timestamp.get(timestamp)
                if codes is None and detail is not None:
                    codes = [str(code) for code in detail.get("codes", [])]
                for code in codes or ():
                    issue_counts[code] -= 1
                    if issue_counts[code] <= 0:
                        issue_counts.pop(code, None)
                integrity_details = [
                    item for item in integrity_details if item.get("timestamp") != iso
                ]
                detail_by_timestamp.pop(iso, None)

            verified_repairs.append(
                {
                    "timestamp": iso,
                    "day": day_text,
                    "path": str(path),
                    "method": "BINANCE_DAILY_AGGTRADES_RECONSTRUCTION",
                }
            )

    repaired_missing = int(scan.get("missing_candles", 0)) - sum(missing_by_day.values())
    repaired_invalid = int(scan.get("invalid_candles", 0)) - sum(invalid_by_day.values())
    result.update(
        missing_by_day=dict(sorted(missing_by_day.items())),
        missing_days=len(missing_by_day),
        missing_candles=sum(missing_by_day.values()),
        found_candles=int(scan.get("found_candles", 0)) + max(0, repaired_missing),
        invalid_by_day=dict(sorted(invalid_by_day.items())),
        invalid_days=len(invalid_by_day),
        invalid_candles=sum(invalid_by_day.values()),
        integrity_issue_counts=dict(sorted(issue_counts.items())),
        integrity_issues=integrity_details,
        verified_repairs=verified_repairs,
        verified_repair_candles=max(0, repaired_invalid),
        archives=list(scan.get("archives", [])) + overlays,
        archives_scanned=int(scan.get("archives_scanned", 0)) + len(overlays),
    )
    result["complete"] = (
        result["missing_candles"] == 0
        and result["invalid_candles"] == 0
        and not result.get("invalid_archives")
    )
    return result


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
    root = Path(root).resolve()
    base = _base_scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start_date,
        end_date,
        progress=progress,
        cancelled=cancelled,
    )
    return _apply_verified_overlays(root, base)


def _eligible_integrity_details(scan: dict) -> list[dict]:
    if scan.get("dataset") != "klines":
        return []
    result = []
    for detail in scan.get("integrity_issues", []):
        codes = {str(code) for code in detail.get("codes", [])}
        if codes and codes <= _AGGTRADES_REPAIRABLE_CODES:
            result.append(detail)
    return result


def _aggtrades_tasks(scan: dict):
    spec = DATASETS["aggTrades"]
    days = sorted(
        {detail["day"] for detail in _eligible_integrity_details(scan) if detail.get("day")}
    )
    tasks = []
    for day_text in days:
        day = date.fromisoformat(day_text)
        tasks.append(_task(spec, scan["symbol"], "daily", day_text, None, day, day))
    return tasks


def _decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"Non-finite decimal value {value!r}")
    return parsed


def _decimal_close(left: Decimal, right: Decimal) -> bool:
    tolerance = max(abs(right) * Decimal("1e-10"), Decimal("1e-8"))
    return abs(left - right) <= tolerance


def _buyer_maker(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "t"}:
        return True
    if normalized in {"false", "0", "f"}:
        return False
    raise ValueError(f"Unrecognized aggTrades buyer-maker flag: {value!r}")


def _read_aggtrades_for_targets(path: Path, targets: set[int], interval_ms: int):
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in _zip_csv_rows(path):
        if not row or len(row) < 7:
            continue
        try:
            agg_trade_id = int(str(row[0]).strip())
        except ValueError:
            # Header row.
            continue
        timestamp = _normalize_epoch_ms(row[5])
        if timestamp is None:
            continue
        bucket = timestamp - (timestamp % interval_ms)
        if bucket not in targets:
            continue
        price = _decimal(row[1])
        quantity = _decimal(row[2])
        first_trade_id = int(str(row[3]).strip())
        last_trade_id = int(str(row[4]).strip())
        if price <= 0 or quantity <= 0 or last_trade_id < first_trade_id:
            raise ValueError("Invalid Binance aggTrades row encountered during verified repair")
        grouped[bucket].append(
            {
                "agg_trade_id": agg_trade_id,
                "price": price,
                "quantity": quantity,
                "first_trade_id": first_trade_id,
                "last_trade_id": last_trade_id,
                "timestamp": timestamp,
                "buyer_maker": _buyer_maker(row[6]),
            }
        )
    return grouped


def _reconstruct_row(
    original: list[str], trades: list[dict], timestamp: int, interval_ms: int
):
    if len(original) < 11 or not trades:
        return None, "source kline or aggTrades schema is incomplete"
    trades = sorted(trades, key=lambda item: (item["timestamp"], item["agg_trade_id"]))

    # Prove the aggregate stream is complete for this candle by checking the
    # constituent trade-id ranges and Binance's own kline trade_count.
    for previous, current in zip(trades, trades[1:]):
        if current["first_trade_id"] != previous["last_trade_id"] + 1:
            return None, "aggTrades individual trade-id ranges are not contiguous"
    trade_count = sum(
        item["last_trade_id"] - item["first_trade_id"] + 1 for item in trades
    )
    try:
        original_trade_count_decimal = _decimal(original[8])
        original_trade_count = int(original_trade_count_decimal)
    except (ValueError, IndexError):
        return None, "source kline trade_count is unavailable"
    if (
        original_trade_count_decimal != Decimal(original_trade_count)
        or trade_count != original_trade_count
    ):
        return None, "aggTrades trade count does not reconcile to the source kline"

    prices = [item["price"] for item in trades]
    volume = sum((item["quantity"] for item in trades), Decimal(0))
    quote_volume = sum(
        (item["price"] * item["quantity"] for item in trades), Decimal(0)
    )
    taker_buy_base = sum(
        (item["quantity"] for item in trades if not item["buyer_maker"]), Decimal(0)
    )
    taker_buy_quote = sum(
        (
            item["price"] * item["quantity"]
            for item in trades
            if not item["buyer_maker"]
        ),
        Decimal(0),
    )
    rebuilt = {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": volume,
        "quote_volume": quote_volume,
        "taker_buy_base_volume": taker_buy_base,
        "taker_buy_quote_volume": taker_buy_quote,
    }

    # Price path must already agree. This fallback is intentionally for volume
    # metadata anomalies, not for rewriting historical prices.
    for name, index in (("open", 1), ("high", 2), ("low", 3), ("close", 4)):
        try:
            if not _decimal_close(_decimal(original[index]), rebuilt[name]):
                return None, f"aggTrades {name} does not reconcile to the source kline"
        except (ValueError, IndexError):
            return None, f"source kline {name} is unavailable"

    matching_volume_fields = []
    for name, index in _VOLUME_FIELD_INDEXES.items():
        try:
            if _decimal_close(_decimal(original[index]), rebuilt[name]):
                matching_volume_fields.append(name)
        except (ValueError, IndexError):
            pass
    if len(matching_volume_fields) < 3:
        return None, "more than one source volume-family field disagrees with aggTrades"

    def text(value: Decimal) -> str:
        return format(value, "f")

    row = [
        str(timestamp),
        text(rebuilt["open"]),
        text(rebuilt["high"]),
        text(rebuilt["low"]),
        text(rebuilt["close"]),
        text(volume),
        str(timestamp + interval_ms - 1),
        text(quote_volume),
        str(trade_count),
        text(taker_buy_base),
        text(taker_buy_quote),
        "0",
    ]
    if _kline_integrity_issues(row, "klines", timestamp, interval_ms):
        return None, "reconstructed candle still fails kline integrity checks"
    return row, matching_volume_fields


def _write_overlay(
    root: Path,
    symbol: str,
    interval: str,
    day: date,
    repaired_rows: dict[int, list[str]],
    repair_records: list[dict],
) -> Path:
    path = _overlay_path(root, symbol, interval, day)
    existing_rows, existing_manifest = _read_overlay(path)
    existing_rows.update(repaired_rows)
    existing_records = {
        int(item["timestamp_ms"]): item
        for item in existing_manifest.get("repairs", [])
        if isinstance(item, dict) and str(item.get("timestamp_ms", "")).isdigit()
    }
    for record in repair_records:
        existing_records[int(record["timestamp_ms"])] = record

    manifest = {
        "version": 1,
        "method": "BINANCE_DAILY_AGGTRADES_RECONSTRUCTION",
        "symbol": symbol,
        "dataset": "klines",
        "interval": interval,
        "utc_day": day.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repairs": [existing_records[key] for key in sorted(existing_records)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.unlink(missing_ok=True)
    csv_name = f"{symbol}-{interval}-{day.isoformat()}.verified-repair.csv"
    csv_text = "\n".join(",".join(existing_rows[key]) for key in sorted(existing_rows)) + "\n"
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, csv_text)
        archive.writestr(
            "repair_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
    os.replace(temporary, path)
    return path


def _reconstruct_from_aggtrades(
    root: Path, scan: dict, *, progress=None, cancelled=None
) -> dict:
    details = _eligible_integrity_details(scan)
    if not details:
        return {
            "reconstructed_candles": 0,
            "verified_repair_days": [],
            "failures": {},
        }

    interval = scan["interval"]
    interval_ms = INTERVAL_MILLISECONDS[interval]
    symbol = scan["symbol"]
    details_by_day: dict[str, list[dict]] = defaultdict(list)
    for detail in details:
        details_by_day[detail["day"]].append(detail)

    reconstructed = 0
    verified_days = []
    failures = {}
    for index, (day_text, day_details) in enumerate(sorted(details_by_day.items()), 1):
        if cancelled and cancelled():
            break
        day = date.fromisoformat(day_text)
        agg_path = _aggtrades_path(root, symbol, day)
        if not agg_path.is_file():
            failures[day_text] = "Binance daily aggTrades archive is unavailable"
            continue
        targets = {
            int(datetime.fromisoformat(detail["timestamp"]).timestamp() * 1000)
            for detail in day_details
        }
        grouped = _read_aggtrades_for_targets(agg_path, targets, interval_ms)
        repaired_rows = {}
        records = []
        for detail in day_details:
            timestamp = int(datetime.fromisoformat(detail["timestamp"]).timestamp() * 1000)
            source_path = Path(detail["source_path"])
            original = _read_kline_rows(source_path, {timestamp}).get(timestamp)
            if original is None:
                failures[detail["timestamp"]] = "source kline row could not be re-read"
                continue
            row, evidence = _reconstruct_row(
                original, grouped.get(timestamp, []), timestamp, interval_ms
            )
            if row is None:
                failures[detail["timestamp"]] = str(evidence)
                continue
            repaired_rows[timestamp] = row
            records.append(
                {
                    "timestamp_ms": timestamp,
                    "timestamp": detail["timestamp"],
                    "original_issue_codes": [
                        str(code) for code in detail.get("codes", [])
                    ],
                    "source_kline_path": str(source_path),
                    "source_kline_sha256": _sha256(source_path),
                    "aggtrades_path": str(agg_path),
                    "aggtrades_sha256": _sha256(agg_path),
                    "matching_volume_fields": list(evidence),
                    "original_row": original,
                    "reconstructed_row": row,
                }
            )
        if repaired_rows:
            _write_overlay(root, symbol, interval, day, repaired_rows, records)
            reconstructed += len(repaired_rows)
            verified_days.append(day_text)
        if progress:
            progress(
                index,
                len(details_by_day),
                {
                    "key": day_text,
                    "status": "reconstructed" if repaired_rows else "unresolved",
                },
            )

    return {
        "reconstructed_candles": reconstructed,
        "verified_repair_days": sorted(verified_days),
        "failures": failures,
    }


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
    """Run ordinary kline repair, then verify unresolved volume anomalies via aggTrades."""
    root = Path(root).resolve()
    before = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start_date,
        end_date,
        progress=(
            (lambda done, total, info: progress("scan-before", done, total, info))
            if progress
            else None
        ),
        cancelled=cancelled,
    )
    if before.get("cancelled") or before.get("complete"):
        return {
            "cancelled": before.get("cancelled", False),
            "before": before,
            "after": before,
            "repair_results": [],
            "monthly_repairs": 0,
            "daily_repairs": 0,
            "aggtrade_repairs": 0,
            "reconstructed_candles": 0,
            "verified_repair_days": [],
            "source_missing_days": [],
            "failed_days": [],
            "unresolved_days": [],
        }

    def base_progress(stage, done, total, detail):
        if progress:
            progress(
                "scan-after-daily" if stage == "scan-after" else stage,
                done,
                total,
                detail,
            )

    base = _base_scan_and_repair_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start_date,
        end_date,
        verify=verify,
        max_connections=max_connections,
        progress=base_progress,
        cancelled=cancelled,
        opener=opener,
    )
    if base.get("cancelled"):
        base["before"] = before
        return base

    after_daily = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start_date,
        end_date,
        cancelled=cancelled,
    )
    agg_tasks = _aggtrades_tasks(after_daily)
    agg_results = _run_repair_tasks(
        agg_tasks,
        root,
        verify=verify,
        max_connections=max_connections,
        progress=(
            (lambda done, total, result: progress("repair-aggtrades", done, total, result))
            if progress
            else None
        ),
        cancelled=cancelled,
        opener=opener,
    )

    reconstruction = _reconstruct_from_aggtrades(
        root,
        after_daily,
        progress=(
            (
                lambda done, total, detail: progress(
                    "reconstruct-aggtrades", done, total, detail
                )
            )
            if progress
            else None
        ),
        cancelled=cancelled,
    )
    after = scan_kline_range(
        root,
        symbol,
        dataset,
        interval,
        start_date,
        end_date,
        progress=(
            (lambda done, total, info: progress("scan-after", done, total, info))
            if progress
            else None
        ),
        cancelled=cancelled,
    )

    source_missing_days = set(base.get("source_missing_days", []))
    failed_days = set(base.get("failed_days", []))
    for result in agg_results:
        day_text = result.task.span_start.isoformat()
        if result.status == "missing":
            source_missing_days.add(day_text)
        elif result.status in {"failed", "cancelled"}:
            failed_days.add(day_text)

    repair_results = list(base.get("repair_results", [])) + list(agg_results)
    unresolved_days = sorted(
        set(after.get("missing_by_day", {})) | set(after.get("invalid_by_day", {}))
    )
    return {
        "cancelled": after.get("cancelled", False),
        "before": before,
        "after": after,
        "repair_results": repair_results,
        "monthly_repairs": int(base.get("monthly_repairs", 0)),
        "daily_repairs": int(base.get("daily_repairs", 0)),
        "aggtrade_repairs": len(agg_results),
        "reconstructed_candles": int(reconstruction["reconstructed_candles"]),
        "verified_repair_days": reconstruction["verified_repair_days"],
        "aggtrade_repair_failures": reconstruction["failures"],
        "source_missing_days": sorted(source_missing_days),
        "failed_days": sorted(failed_days),
        "unresolved_days": unresolved_days,
    }
