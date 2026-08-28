from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from binance_data_hub.verified_repair import (
    _aggtrades_tasks,
    _overlay_path,
    _reconstruct_from_aggtrades,
    scan_kline_range,
)


def _write_bad_kline_day(root: Path, symbol: str, day: date, period: str):
    interval = "1m"
    key = day.isoformat() if period == "daily" else day.strftime("%Y-%m")
    path = (
        root
        / "raw"
        / "futures"
        / "um"
        / period
        / "klines"
        / symbol
        / interval
        / f"{symbol}-{interval}-{key}.zip"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    base = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    rows = []
    for index in range(1440):
        timestamp = base + index * 60_000
        if index == 755:  # 12:35 UTC
            # Everything except total base volume agrees with the aggTrades
            # reconstruction used below.
            row = f"{timestamp},1,1,1,1,10,{timestamp + 59_999},20,2,12,12,0"
        else:
            row = f"{timestamp},1,1,1,1,10,{timestamp + 59_999},10,1,5,5,0"
        rows.append(row)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.stem + ".csv", "\n".join(rows) + "\n")
    return path


def _write_daily_aggtrades(root: Path, symbol: str, day: date, *, trade_count_ok=True):
    path = (
        root
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / "aggTrades"
        / symbol
        / f"{symbol}-aggTrades-{day.isoformat()}.zip"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    base = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    timestamp = base + 755 * 60_000
    if trade_count_ok:
        rows = [
            f"1,1,12,100,100,{timestamp + 1000},false",
            f"2,1,8,101,101,{timestamp + 2000},true",
        ]
    else:
        rows = [f"1,1,20,100,100,{timestamp + 1000},false"]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.stem + ".csv", "\n".join(rows) + "\n")
    return path


def _overlay_rows(path: Path):
    with zipfile.ZipFile(path) as archive:
        csv_member = next(name for name in archive.namelist() if name.endswith(".csv"))
        rows = list(csv.reader(io.TextIOWrapper(archive.open(csv_member), encoding="utf-8")))
        manifest = json.loads(archive.read("repair_manifest.json"))
    return rows, manifest


def test_unresolved_bad_daily_kline_can_be_verified_from_daily_aggtrades(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    monthly = _write_bad_kline_day(tmp_path, symbol, day, "monthly")
    daily = _write_bad_kline_day(tmp_path, symbol, day, "daily")
    monthly_bytes = monthly.read_bytes()
    daily_bytes = daily.read_bytes()
    _write_daily_aggtrades(tmp_path, symbol, day)

    before = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    assert before["missing_candles"] == 0
    assert before["invalid_candles"] == 1
    assert before["integrity_issue_counts"] == {"TAKER_VOLUME_EXCEEDS_TOTAL": 1}

    result = _reconstruct_from_aggtrades(tmp_path, before)

    assert result["reconstructed_candles"] == 1
    assert result["verified_repair_days"] == [day.isoformat()]
    overlay = _overlay_path(tmp_path, symbol, "1m", day)
    assert overlay.is_file()

    rows, manifest = _overlay_rows(overlay)
    assert len(rows) == 1
    assert rows[0][5] == "20"
    assert rows[0][7] == "20"
    assert rows[0][8] == "2"
    assert rows[0][9] == "12"
    assert manifest["method"] == "BINANCE_DAILY_AGGTRADES_RECONSTRUCTION"
    assert manifest["repairs"][0]["original_issue_codes"] == ["TAKER_VOLUME_EXCEEDS_TOTAL"]
    assert manifest["repairs"][0]["matching_volume_fields"] == [
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    after = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    assert after["invalid_candles"] == 0
    assert after["complete"] is True
    assert after["verified_repair_candles"] == 1
    assert monthly.read_bytes() == monthly_bytes
    assert daily.read_bytes() == daily_bytes


def test_aggtrades_fallback_is_targeted_to_the_exact_invalid_utc_day(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    _write_bad_kline_day(tmp_path, symbol, day, "monthly")
    _write_bad_kline_day(tmp_path, symbol, day, "daily")

    scan = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    tasks = _aggtrades_tasks(scan)

    assert len(tasks) == 1
    assert tasks[0].dataset == "aggTrades"
    assert tasks[0].period == "daily"
    assert tasks[0].key == "2023-11-30"
    assert tasks[0].interval is None


def test_verified_repair_refuses_incomplete_aggtrades_evidence(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    _write_bad_kline_day(tmp_path, symbol, day, "monthly")
    _write_bad_kline_day(tmp_path, symbol, day, "daily")
    _write_daily_aggtrades(tmp_path, symbol, day, trade_count_ok=False)

    before = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    result = _reconstruct_from_aggtrades(tmp_path, before)

    assert result["reconstructed_candles"] == 0
    assert result["failures"]
    assert not _overlay_path(tmp_path, symbol, "1m", day).exists()
    after = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    assert after["invalid_candles"] == 1
    assert after["complete"] is False
