from __future__ import annotations

import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import binance_data_hub.repair as repair_module
from binance_data_hub.repair import (
    _daily_repair_tasks,
    _monthly_repair_tasks,
    scan_aggtrade_range,
    scan_and_repair_aggtrade_range,
    scan_kline_range,
)


def _month_end(day: date) -> date:
    probe = day.replace(day=28) + timedelta(days=4)
    return probe.replace(day=1) - timedelta(days=1)


def _write_monthly_1m(root: Path, symbol: str, month: date, start: date, end: date, missing_days: set[date]):
    dataset = "klines"
    interval = "1m"
    path = (
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
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    day = max(start, month)
    last = min(end, _month_end(month))
    while day <= last:
        if day not in missing_days:
            base = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            rows.extend(f"{base + index * 60_000},1,1,1,1,1\n" for index in range(1440))
        day += timedelta(days=1)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.stem + ".csv", "".join(rows))
    return path


def _write_daily_1m(root: Path, symbol: str, day: date):
    dataset = "klines"
    interval = "1m"
    path = (
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
    path.parent.mkdir(parents=True, exist_ok=True)
    base = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            path.stem + ".csv",
            "".join(f"{base + index * 60_000},1,1,1,1,1\n" for index in range(1440)),
        )
    return path


def _write_monthly_with_bad_taker(root: Path, symbol: str, day: date, bad_index: int = 755):
    path = (
        root
        / "raw"
        / "futures"
        / "um"
        / "monthly"
        / "klines"
        / symbol
        / "1m"
        / f"{symbol}-1m-{day.strftime('%Y-%m')}.zip"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    base = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    rows = []
    for index in range(1440):
        timestamp = base + index * 60_000
        volume = 10
        taker_base = 12 if index == bad_index else 5
        rows.append(
            f"{timestamp},1,1,1,1,{volume},{timestamp + 59_999},20,1,{taker_base},8,0\n"
        )
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.stem + ".csv", "".join(rows))
    return path


def test_scan_detects_five_missing_days_as_7200_one_minute_candles(tmp_path):
    symbol = "BTCUSDT"
    start = date(2022, 2, 26)
    end = date(2022, 4, 2)
    missing = {
        date(2022, 3, 1),
        date(2022, 3, 7),
        date(2022, 3, 14),
        date(2022, 3, 21),
        date(2022, 3, 28),
    }

    for month in (date(2022, 2, 1), date(2022, 3, 1), date(2022, 4, 1)):
        _write_monthly_1m(tmp_path, symbol, month, start, end, missing)

    result = scan_kline_range(tmp_path, symbol, "klines", "1m", start, end)

    assert result["expected_candles"] == 51_840
    assert result["missing_candles"] == 7_200
    assert result["missing_days"] == 5
    assert set(result["missing_by_day"]) == {day.isoformat() for day in missing}
    assert set(result["missing_by_day"].values()) == {1440}


def test_daily_repair_archive_logically_supplements_monthly_without_modifying_it(tmp_path):
    symbol = "BTCUSDT"
    start = date(2022, 3, 1)
    end = date(2022, 3, 3)
    missing_day = date(2022, 3, 2)
    monthly = _write_monthly_1m(tmp_path, symbol, date(2022, 3, 1), start, end, {missing_day})
    original_bytes = monthly.read_bytes()

    before = scan_kline_range(tmp_path, symbol, "klines", "1m", start, end)
    assert before["missing_candles"] == 1440

    _write_daily_1m(tmp_path, symbol, missing_day)
    after = scan_kline_range(tmp_path, symbol, "klines", "1m", start, end)

    assert after["missing_candles"] == 0
    assert after["complete"] is True
    assert monthly.read_bytes() == original_bytes


def test_valid_existing_monthly_archive_with_internal_gap_is_not_planned_for_redownload(tmp_path):
    symbol = "BTCUSDT"
    start = date(2022, 3, 1)
    end = date(2022, 3, 3)
    _write_monthly_1m(tmp_path, symbol, date(2022, 3, 1), start, end, {date(2022, 3, 2)})

    scan = scan_kline_range(tmp_path, symbol, "klines", "1m", start, end)
    tasks = _monthly_repair_tasks(tmp_path, scan, date(2026, 8, 25))

    assert tasks == []


def test_missing_monthly_archive_is_preferred_before_daily_fallback(tmp_path):
    scan = {
        "dataset": "klines",
        "symbol": "BTCUSDT",
        "interval": "1m",
        "start_date": "2022-03-01",
        "end_date": "2022-03-03",
        "missing_by_day": {"2022-03-01": 1440, "2022-03-02": 1440, "2022-03-03": 1440},
        "invalid_archives": [],
    }

    tasks = _monthly_repair_tasks(tmp_path, scan, date(2026, 8, 25))

    assert len(tasks) == 1
    assert tasks[0].period == "monthly"
    assert tasks[0].key == "2022-03"


def test_scan_detects_present_candle_with_taker_volume_above_total(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    _write_monthly_with_bad_taker(tmp_path, symbol, day)

    result = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)

    assert result["missing_candles"] == 0
    assert result["invalid_candles"] == 1
    assert result["invalid_by_day"] == {day.isoformat(): 1}
    assert result["integrity_issue_counts"] == {"TAKER_VOLUME_EXCEEDS_TOTAL": 1}
    assert result["integrity_issues"][0]["timestamp"] == "2023-11-30T12:35:00+00:00"
    assert result["complete"] is False


def test_invalid_monthly_candle_plans_targeted_daily_repair(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    _write_monthly_with_bad_taker(tmp_path, symbol, day)

    scan = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    tasks = _daily_repair_tasks(scan)

    assert len(tasks) == 1
    assert tasks[0].period == "daily"
    assert tasks[0].key == day.isoformat()


def test_valid_daily_row_overrides_bad_monthly_row_without_modifying_monthly(tmp_path):
    symbol = "XRPUSDT"
    day = date(2023, 11, 30)
    monthly = _write_monthly_with_bad_taker(tmp_path, symbol, day)
    original_bytes = monthly.read_bytes()

    before = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)
    assert before["invalid_candles"] == 1

    _write_daily_1m(tmp_path, symbol, day)
    after = scan_kline_range(tmp_path, symbol, "klines", "1m", day, day)

    assert after["missing_candles"] == 0
    assert after["invalid_candles"] == 0
    assert after["complete"] is True
    assert monthly.read_bytes() == original_bytes



def _aggtrade_monthly_path(root: Path, symbol: str, month: str) -> Path:
    return (
        root
        / "raw"
        / "futures"
        / "um"
        / "monthly"
        / "aggTrades"
        / symbol
        / f"{symbol}-aggTrades-{month}.zip"
    )


def _aggtrade_daily_path(root: Path, symbol: str, day: date) -> Path:
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


def _write_aggtrade_archive(path: Path, rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.stem + ".csv", "".join(rows))
    return path


def test_aggtrade_scan_uses_daily_fallback_when_monthly_source_is_absent(tmp_path):
    symbol = "BTCUSDT"
    start = date(2022, 9, 1)
    end = date(2022, 9, 2)
    for offset, day in enumerate((start, end)):
        _write_aggtrade_archive(
            _aggtrade_daily_path(tmp_path, symbol, day),
            [
                f"{100 + offset},20000,0.01,{500 + offset},{500 + offset},"
                f"{1661990400000 + offset * 86400000},true\n"
            ],
        )

    result = scan_aggtrade_range(tmp_path, symbol, start, end)

    assert result["archives_scanned"] == 2
    assert result["missing_archives"] == 0
    assert result["invalid_archives_count"] == 0
    assert result["complete"] is True


def test_aggtrade_repair_falls_back_to_daily_if_official_monthly_stays_invalid(
    tmp_path, monkeypatch
):
    symbol = "BTCUSDT"
    start = date(2022, 9, 1)
    end = date(2022, 9, 2)
    monthly = _aggtrade_monthly_path(tmp_path, symbol, "2022-09")
    _write_aggtrade_archive(
        monthly,
        [
            "100,20000,0.01,500,500,1661990400000,true\n",
            "100,20001,0.02,501,501,1661990401000,false\n",
        ],
    )

    calls = []

    def fake_run(tasks, root, **_kwargs):
        calls.append([task.period for task in tasks])
        results = []
        for index, task in enumerate(tasks):
            path = Path(root) / task.relative_path
            if task.period == "monthly":
                _write_aggtrade_archive(
                    path,
                    [
                        "100,20000,0.01,500,500,1661990400000,true\n",
                        "100,20001,0.02,501,501,1661990401000,false\n",
                    ],
                )
            else:
                agg_id = 200 + index
                trade_id = 600 + index
                timestamp = int(
                    datetime.combine(
                        task.span_start, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                    * 1000
                )
                _write_aggtrade_archive(
                    path,
                    [
                        f"{agg_id},20000,0.01,{trade_id},{trade_id},"
                        f"{timestamp},true\n"
                    ],
                )
            results.append(SimpleNamespace(status="downloaded", task=task))
        return results

    monkeypatch.setattr(repair_module, "_run_repair_tasks", fake_run)

    result = scan_and_repair_aggtrade_range(
        tmp_path, symbol, start, end, max_connections=2
    )

    assert result["before"]["invalid_archives_count"] == 1
    assert result["after"]["complete"] is True
    assert result["daily_fallback_repairs"] == 2
    assert not monthly.exists()
    assert calls == [["monthly"], ["daily", "daily"]]


def test_aggtrade_repair_scan_fully_checks_zip_integrity(tmp_path):
    symbol = "BTCUSDT"
    path = _aggtrade_monthly_path(tmp_path, symbol, "2022-03")
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{symbol}-aggTrades-2022-03.csv",
            "1,43000,0.01,10,10,1646092800000,true\n",
        )

    result = scan_aggtrade_range(
        tmp_path, symbol, date(2022, 3, 1), date(2022, 3, 31)
    )

    assert result["archives_scanned"] == 1
    assert result["missing_archives"] == 0
    assert result["invalid_archives_count"] == 0
    assert result["complete"] is True


def test_aggtrade_repair_scan_rejects_duplicate_aggregate_trade_ids(tmp_path):
    symbol = "BTCUSDT"
    path = _aggtrade_monthly_path(tmp_path, symbol, "2022-09")
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{symbol}-aggTrades-2022-09.csv",
            "".join(
                [
                    "100,20000,0.01,500,500,1661990400000,true\n",
                    "101,20001,0.02,501,501,1661990401000,false\n",
                    "101,20002,0.03,502,502,1661990402000,true\n",
                ]
            ),
        )

    result = scan_aggtrade_range(
        tmp_path, symbol, date(2022, 9, 1), date(2022, 9, 30)
    )

    assert result["missing_archives"] == 0
    assert result["invalid_archives_count"] == 1
    assert result["invalid_archive_keys"] == ["monthly:2022-09"]
    assert "duplicate aggTradeId 101" in result["invalid_archives"][0]["error"]
    assert result["complete"] is False


def test_aggtrade_repair_scan_rejects_invalid_event_geometry(tmp_path):
    symbol = "BTCUSDT"
    path = _aggtrade_monthly_path(tmp_path, symbol, "2022-09")
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{symbol}-aggTrades-2022-09.csv",
            "100,20000,0.01,501,500,1661990400000,true\n",
        )

    result = scan_aggtrade_range(
        tmp_path, symbol, date(2022, 9, 1), date(2022, 9, 30)
    )

    assert result["invalid_archives_count"] == 1
    assert "lastTradeId is below firstTradeId" in result["invalid_archives"][0]["error"]
    assert result["complete"] is False


def test_aggtrade_repair_scan_reports_corrupt_archive(tmp_path):
    symbol = "BTCUSDT"
    path = _aggtrade_monthly_path(tmp_path, symbol, "2022-03")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not a zip archive")

    result = scan_aggtrade_range(
        tmp_path, symbol, date(2022, 3, 1), date(2022, 3, 31)
    )

    assert result["missing_archives"] == 0
    assert result["invalid_archives_count"] == 1
    assert result["invalid_archive_keys"] == ["monthly:2022-03"]
    assert result["complete"] is False
