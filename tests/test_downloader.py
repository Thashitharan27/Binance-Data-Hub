import hashlib
import io
from datetime import date
from urllib.error import HTTPError

from binance_data_hub.archive_downloader import (
    _download_one,
    download_archive_library,
    plan_archive_tasks,
)


class Response(io.BytesIO):
    def __init__(self, payload=b"", status=200):
        super().__init__(payload)
        self.status = status
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def getcode(self):
        return self.status


def test_plan_uses_monthly_for_completed_months_and_daily_for_current_month():
    tasks = plan_archive_tasks(
        "BTCUSDT",
        ["klines"],
        ["1h"],
        "2026-06-15",
        "2026-08-20",
        today=date(2026, 8, 21),
    )
    assert [(t.period, t.key) for t in tasks[:2]] == [
        ("monthly", "2026-06"),
        ("monthly", "2026-07"),
    ]
    daily = [t for t in tasks if t.period == "daily"]
    assert daily[0].key == "2026-08-01"
    assert daily[-1].key == "2026-08-20"
    assert len(daily) == 20


def test_metrics_are_daily_only():
    tasks = plan_archive_tasks(
        "ETHUSDT",
        ["metrics"],
        ["1m"],
        "2026-08-18",
        "2026-08-20",
        today=date(2026, 8, 21),
    )
    assert [(t.period, t.key, t.interval) for t in tasks] == [
        ("daily", "2026-08-18", None),
        ("daily", "2026-08-19", None),
        ("daily", "2026-08-20", None),
    ]


def test_funding_rate_uses_monthly_archive_only():
    tasks = plan_archive_tasks(
        "BTCUSDT",
        ["fundingRate"],
        [],
        "2026-06-01",
        "2026-08-20",
        today=date(2026, 8, 21),
    )
    assert [(t.period, t.key) for t in tasks] == [
        ("monthly", "2026-06"),
        ("monthly", "2026-07"),
    ]


def test_download_streams_zip_and_skips_existing(tmp_path):
    payload = b"PK" + b"x" * 100
    expected = hashlib.sha256(payload).hexdigest()
    task = plan_archive_tasks(
        "BTCUSDT", ["klines"], ["1h"], "2026-07-01", "2026-07-31", today=date(2026, 8, 21)
    )[0]

    def opener(request, **_kwargs):
        if request.full_url.endswith(".CHECKSUM"):
            return Response(f"{expected}  file.zip\n".encode())
        return Response(payload)

    result = _download_one(task, tmp_path, verify=True, cancelled=lambda: False, opener=opener)
    assert result.status == "downloaded"
    path = tmp_path / task.relative_path
    assert path.read_bytes() == payload

    result2 = _download_one(task, tmp_path, verify=False, cancelled=lambda: False, opener=opener)
    assert result2.status == "skipped"


def test_404_is_missing_not_failed(tmp_path):
    task = plan_archive_tasks(
        "BTCUSDT", ["metrics"], [], "2026-08-20", "2026-08-20", today=date(2026, 8, 21)
    )[0]

    def opener(request, **_kwargs):
        raise HTTPError(request.full_url, 404, "missing", {}, None)

    result = _download_one(task, tmp_path, verify=False, cancelled=lambda: False, opener=opener)
    assert result.status == "missing"


def test_completed_aggtrade_monthly_download_removes_obsolete_daily_overlap(tmp_path):
    payload = b"PK" + b"a" * 64
    daily_dir = (
        tmp_path
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / "aggTrades"
        / "BTCUSDT"
    )
    daily_dir.mkdir(parents=True, exist_ok=True)
    obsolete = daily_dir / "BTCUSDT-aggTrades-2026-08-02.zip"
    obsolete.write_bytes(b"old-daily")
    older_month = daily_dir / "BTCUSDT-aggTrades-2026-07-31.zip"
    older_month.write_bytes(b"keep")

    def opener(request, **_kwargs):
        return Response(payload)

    summary = download_archive_library(
        "BTCUSDT",
        ["aggTrades"],
        [],
        tmp_path,
        "2026-08-01",
        "2026-08-31",
        workers=1,
        verify=False,
        opener=opener,
        today=date(2026, 9, 2),
    )

    assert summary["counts"]["downloaded"] == 1
    assert not obsolete.exists()
    assert older_month.exists()
    assert summary["removed_superseded_daily"] == [str(obsolete)]


def test_monthly_kline_download_does_not_remove_daily_repair_source(tmp_path):
    payload = b"PK" + b"k" * 64
    daily_dir = (
        tmp_path
        / "raw"
        / "futures"
        / "um"
        / "daily"
        / "klines"
        / "BTCUSDT"
        / "1m"
    )
    daily_dir.mkdir(parents=True, exist_ok=True)
    repair_daily = daily_dir / "BTCUSDT-1m-2026-08-02.zip"
    repair_daily.write_bytes(b"repair-daily")

    def opener(request, **_kwargs):
        return Response(payload)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-08-01",
        "2026-08-31",
        workers=1,
        verify=False,
        opener=opener,
        today=date(2026, 9, 2),
    )

    assert summary["counts"]["downloaded"] == 1
    assert repair_daily.exists()
    assert summary["removed_superseded_daily"] == []


def test_multi_symbol_collection_plans_in_one_run(tmp_path):
    payload = b"PK" + b"z" * 32

    def opener(request, **_kwargs):
        return Response(payload)

    summary = download_archive_library(
        ["BTCUSDT", "ETHUSDT"],
        ["metrics"],
        [],
        tmp_path,
        "2026-08-20",
        "2026-08-20",
        workers=2,
        verify=False,
        opener=opener,
        today=date(2026, 8, 21),
    )
    assert summary["planned"] == 2
    assert summary["counts"]["downloaded"] == 2
    assert {r.task.symbol for r in summary["results"]} == {"BTCUSDT", "ETHUSDT"}
