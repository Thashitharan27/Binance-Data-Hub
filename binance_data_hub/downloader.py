"""Automatic, resumable Binance USD-M Futures OHLCV downloads.

Complete historical months come from Binance's official public-data archives and
are fetched concurrently. Missing archives and the current partial month fall
back to the REST API. The destination is replaced only after every part has
been merged and validated.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
ARCHIVE_ROOT = "https://data.binance.vision/data/futures/um/monthly/klines"
INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}
COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")
DEFAULT_ARCHIVE_WORKERS = 4


def _utc_ms(value, end_of_day=False):
    if not value:
        return None
    stamp = pd.Timestamp(value)
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    if end_of_day and len(value.strip()) <= 10:
        stamp += pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
    return int(stamp.timestamp() * 1000)


def _last(path):
    path = Path(path)
    if not path.exists() or not path.stat().st_size:
        return None
    frame = pd.read_csv(path, usecols=["timestamp"])
    if frame.empty:
        return None
    value = frame.timestamp.iloc[-1]
    return int(pd.Timestamp(value).timestamp() * 1000) if isinstance(value, str) and not value.strip().isdigit() else int(value)


def _request(params, opener=urlopen, retries=5):
    for attempt in range(retries):
        try:
            request = Request(
                f"{BASE_URL}?{urlencode(params)}",
                headers={"User-Agent": "Binance-Futures-Data-Hub/2.0"},
            )
            with opener(request, timeout=30) as response:
                payload = json.loads(response.read().decode())
            if isinstance(payload, dict):
                raise RuntimeError(payload.get("msg", str(payload)))
            return payload
        except HTTPError as exc:
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                raise RuntimeError(f"Binance returned HTTP {exc.code}") from exc
            time.sleep(float(exc.headers.get("Retry-After", min(2**attempt, 10))))
        except URLError as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Could not reach Binance: {exc.reason}") from exc
            time.sleep(min(2**attempt, 10))
    return []


def _fetch_bytes(url, opener=urlopen, retries=4):
    """Fetch a public archive resource, returning None only for HTTP 404."""
    for attempt in range(retries):
        try:
            request = Request(url, headers={"User-Agent": "Binance-Futures-Data-Hub/2.0"})
            with opener(request, timeout=60) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code == 404:
                return None
            if exc.code not in (418, 429, 500, 502, 503, 504) or attempt == retries - 1:
                raise RuntimeError(f"Binance archive returned HTTP {exc.code}") from exc
            time.sleep(float(exc.headers.get("Retry-After", min(2**attempt, 10))))
        except URLError as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Could not reach Binance archive: {exc.reason}") from exc
            time.sleep(min(2**attempt, 10))
    return None


def _month_floor(timestamp_ms):
    stamp = pd.to_datetime(timestamp_ms, unit="ms", utc=True)
    return pd.Timestamp(year=stamp.year, month=stamp.month, day=1, tz="UTC")


def _archive_months(start_ms, end_ms):
    """Return complete, published calendar months intersecting the request."""
    current_month = pd.Timestamp.now(tz="UTC").normalize().replace(day=1)
    month = _month_floor(start_ms)
    months = []
    while month < current_month:
        next_month = month + pd.offsets.MonthBegin(1)
        month_end = int(next_month.timestamp() * 1000) - 1
        if month_end > end_ms:
            break
        months.append((month, int(month.timestamp() * 1000), month_end))
        month = next_month
    return months


def _archive_url(symbol, interval, month):
    key = month.strftime("%Y-%m")
    name = f"{symbol}-{interval}-{key}.zip"
    return f"{ARCHIVE_ROOT}/{symbol}/{interval}/{name}"


def _write_archive_part(symbol, interval, month, start_ms, end_ms, part, opener, cancelled):
    """Download, checksum, extract and atomically publish one archive part."""
    if part.exists() and part.stat().st_size:
        return part, max(0, sum(1 for _ in part.open(encoding="utf-8")) - 1), True
    if cancelled and cancelled():
        raise InterruptedError("Download paused; completed parts will resume next time.")
    url = _archive_url(symbol, interval, month)
    payload = _fetch_bytes(url, opener)
    checksum_payload = _fetch_bytes(f"{url}.CHECKSUM", opener) if payload is not None else None
    if payload is None or checksum_payload is None:
        return part, 0, False
    expected = checksum_payload.decode("utf-8-sig").strip().split()[0].lower()
    actual = hashlib.sha256(payload).hexdigest().lower()
    if actual != expected:
        raise RuntimeError(f"Checksum failed for {symbol} {interval} {month:%Y-%m}.")

    part.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{part.name}.", suffix=".tmp", dir=part.parent)
    os.close(fd)
    temporary = Path(temp_name)
    count = 0
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(members) != 1:
                raise RuntimeError(f"Unexpected archive layout for {symbol} {interval} {month:%Y-%m}.")
            with archive.open(members[0]) as raw, temporary.open("w", encoding="utf-8", newline="") as output:
                reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
                writer = csv.writer(output, lineterminator="\n")
                writer.writerow(COLUMNS)
                for row in reader:
                    if not row:
                        continue
                    try:
                        timestamp = int(row[0])
                    except ValueError:  # Some archives contain a header row.
                        continue
                    if start_ms <= timestamp <= end_ms:
                        writer.writerow(row[:6])
                        count += 1
                output.flush()
                os.fsync(output.fileno())
        os.replace(temporary, part)
    finally:
        temporary.unlink(missing_ok=True)
    return part, count, True


def _download_rest_part(symbol, interval, start_ms, end_ms, part, *, progress=None, cancelled=None, opener=urlopen):
    """Download one bounded REST segment with a persistent page checkpoint."""
    if part.exists() and part.stat().st_size:
        return part, max(0, sum(1 for _ in part.open(encoding="utf-8")) - 1)
    step = INTERVAL_MS[interval]
    checkpoint = part.with_name(f".{part.name}.download")
    checkpoint_last = _last(checkpoint)
    cursor = max(start_ms, checkpoint_last + step) if checkpoint_last is not None else start_ms
    added = max(0, sum(1 for _ in checkpoint.open(encoding="utf-8")) - 1) if checkpoint.exists() else 0
    mode = "a" if checkpoint.exists() and checkpoint.stat().st_size else "w"
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    with checkpoint.open(mode, encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        if mode == "w":
            writer.writerow(COLUMNS)
        while cursor <= end_ms:
            if cancelled and cancelled():
                raise InterruptedError("Download paused; saved pages will resume next time.")
            rows = _request(
                {"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end_ms, "limit": 1000},
                opener,
            )
            if not rows:
                break
            written = 0
            for row in rows:
                timestamp = int(row[0])
                if cursor <= timestamp <= end_ms:
                    writer.writerow((row[0], row[1], row[2], row[3], row[4], row[5]))
                    added += 1
                    written += 1
            next_cursor = int(rows[-1][0]) + step
            if next_cursor <= cursor:
                raise RuntimeError("Binance returned a non-advancing page.")
            cursor = next_cursor
            stream.flush()
            os.fsync(stream.fileno())
            if progress:
                progress(added, pd.to_datetime(rows[-1][0], unit="ms", utc=True).strftime("%Y-%m-%d %H:%M UTC"))
            if len(rows) < 1000 or written == 0:
                break
    os.replace(checkpoint, part)
    return part, added


def _merge_parts(destination, sources, step, original_last):
    """Merge ordered sources, deduplicate timestamps and validate ordering."""
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    os.close(fd)
    temporary = Path(temp_name)
    last_timestamp = None
    total = 0
    added = 0
    gaps = 0
    try:
        with temporary.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(COLUMNS)
            for source in sources:
                if not source.exists() or not source.stat().st_size:
                    continue
                with source.open(encoding="utf-8", newline="") as stream:
                    reader = csv.reader(stream)
                    for row in reader:
                        if not row:
                            continue
                        try:
                            timestamp = int(row[0])
                        except ValueError:
                            continue
                        if last_timestamp is not None and timestamp <= last_timestamp:
                            continue
                        if last_timestamp is not None and timestamp > last_timestamp + step:
                            gaps += 1
                        writer.writerow(row[:6])
                        last_timestamp = timestamp
                        total += 1
                        if original_last is None or timestamp > original_last:
                            added += 1
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return total, added, gaps


def download_klines(
    symbol,
    interval,
    destination,
    start_date=None,
    end_date=None,
    *,
    progress=None,
    progress_state=None,
    cancelled=None,
    opener=urlopen,
    archive_opener=None,
    archive_workers=DEFAULT_ARCHIVE_WORKERS,
    use_archives=None,
):
    """Create or update a candle CSV without exposing partial data to readers.

    ``use_archives`` defaults to true for normal operation. It defaults to false
    when a custom REST opener is injected, preserving compatibility with older
    callers and lightweight REST-only tests.
    """
    symbol = symbol.strip().upper().replace("/", "")
    if not symbol or not symbol.isalnum():
        raise ValueError("Enter a valid pair such as BTCUSDT.")
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported timeframe: {interval}")
    if archive_workers < 1:
        raise ValueError("archive_workers must be at least 1")

    step = INTERVAL_MS[interval]
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    original_last = _last(destination)
    legacy_checkpoint = destination.with_name(f".{destination.name}.download")
    checkpoint_last = _last(legacy_checkpoint)
    saved_values = [value for value in (original_last, checkpoint_last) if value is not None]
    saved = max(saved_values) if saved_values else None

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    end = min(_utc_ms(end_date, True) or (now // step) * step - 1, (now // step) * step - 1)
    cursor = saved + step if saved is not None else (_utc_ms(start_date) or 0)
    if cursor > end and not legacy_checkpoint.exists():
        total = max(0, sum(1 for _ in destination.open(encoding="utf-8")) - 1) if destination.exists() else 0
        return {"path": str(destination), "added": 0, "total": total, "gaps": 0, "archives": 0, "rest_segments": 0}

    # Avoid attempting archive months before a contract's first available candle.
    if saved is None:
        first = _request({"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end, "limit": 1}, opener)
        if not first:
            return {"path": str(destination), "added": 0, "total": 0, "gaps": 0, "archives": 0, "rest_segments": 0}
        cursor = max(cursor, int(first[0][0]))

    expected_candles = max(1, (end - cursor) // step + 1)

    def emit_progress(completed, detail):
        if progress:
            progress(completed, detail)
        if progress_state:
            progress_state(completed, expected_candles, detail)

    if use_archives is None:
        use_archives = opener is urlopen
    archive_opener = archive_opener or opener
    parts_dir = destination.with_name(f".{destination.name}.parts")
    parts_dir.mkdir(parents=True, exist_ok=True)
    month_specs = _archive_months(cursor, end) if use_archives else []
    archive_parts = {}
    unavailable_months = []
    prepared = 0
    progress_lock = Lock()

    def report(count, detail):
        nonlocal prepared
        with progress_lock:
            prepared += count
            emit_progress(prepared, detail)

    def archive_job(spec):
        month, month_start, month_end = spec
        segment_start = max(cursor, month_start)
        # Include the requested lower bound so a paused mid-month request is
        # never mistaken for a complete-month part after the user changes dates.
        part = parts_dir / f"archive-{month:%Y-%m}-{segment_start}.csv"
        return month, segment_start, month_end, _write_archive_part(
            symbol,
            interval,
            month,
            segment_start,
            month_end,
            part,
            archive_opener,
            cancelled,
        )

    if month_specs:
        with ThreadPoolExecutor(max_workers=min(archive_workers, len(month_specs))) as pool:
            futures = [pool.submit(archive_job, spec) for spec in month_specs]
            for future in as_completed(futures):
                month, segment_start, month_end, (part, count, available) = future.result()
                if available:
                    archive_parts[month] = part
                    report(count, f"verified archive {month:%Y-%m}")
                else:
                    unavailable_months.append((month, segment_start, month_end))

    segment_parts = []
    for month, month_start, month_end in month_specs:
        if month in archive_parts:
            segment_parts.append((month_start, archive_parts[month]))
            continue
        segment_start = max(cursor, month_start)
        part = parts_dir / f"rest-{month:%Y-%m}-{segment_start}.csv"
        rest_part, count = _download_rest_part(
            symbol,
            interval,
            segment_start,
            month_end,
            part,
            progress=lambda n, detail, base=prepared: emit_progress(base + n, detail),
            cancelled=cancelled,
            opener=opener,
        )
        prepared += count
        segment_parts.append((month_start, rest_part))

    if month_specs:
        tail_start = month_specs[-1][2] + 1
    else:
        tail_start = cursor
    rest_segments = len(unavailable_months)
    if tail_start <= end:
        tail = parts_dir / "rest-tail.csv"
        rest_part, count = _download_rest_part(
            symbol,
            interval,
            tail_start,
            end,
            tail,
            progress=lambda n, detail, base=prepared: emit_progress(base + n, detail),
            cancelled=cancelled,
            opener=opener,
        )
        prepared += count
        segment_parts.append((tail_start, rest_part))
        rest_segments += 1

    if cancelled and cancelled():
        raise InterruptedError("Download paused; completed parts will resume next time.")

    sources = []
    if destination.exists():
        sources.append(destination)
    if legacy_checkpoint.exists():
        sources.append(legacy_checkpoint)
    sources.extend(path for _, path in sorted(segment_parts, key=lambda item: item[0]))
    total, added, gaps = _merge_parts(destination, sources, step, original_last)
    legacy_checkpoint.unlink(missing_ok=True)
    shutil.rmtree(parts_dir, ignore_errors=True)
    return {
        "path": str(destination),
        "added": added,
        "total": total,
        "gaps": gaps,
        "archives": len(archive_parts),
        "rest_segments": rest_segments,
    }
