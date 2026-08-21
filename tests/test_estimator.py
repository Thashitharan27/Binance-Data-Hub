import io
from datetime import date
from urllib.error import HTTPError

from binance_data_hub.archive_downloader import plan_archive_tasks
from binance_data_hub.estimator import estimate_archive_library


class Response(io.BytesIO):
    def __init__(self, payload=b"", status=200, headers=None):
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def getcode(self):
        return self.status


def test_estimate_uses_content_length_without_downloading_body(tmp_path):
    calls = []

    def opener(request, **_kwargs):
        calls.append((request.get_method(), request.full_url))
        return Response(status=200, headers={"Content-Length": "1000"})

    summary = estimate_archive_library(
        "BTCUSDT",
        ["aggTrades"],
        [],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["remaining_bytes"] == 1000
    assert summary["remaining_files"] == 1
    assert calls == [("HEAD", calls[0][1])]


def test_estimate_subtracts_existing_partial_bytes(tmp_path):
    task = plan_archive_tasks(
        "BTCUSDT", ["aggTrades"], [], "2026-07-01", "2026-07-31", today=date(2026, 8, 21)
    )[0]
    part = tmp_path / task.relative_path
    part.parent.mkdir(parents=True, exist_ok=True)
    partial = part.with_name(f"{part.name}.part")
    partial.write_bytes(b"x" * 400)

    def opener(_request, **_kwargs):
        return Response(status=200, headers={"Content-Length": "1000"})

    summary = estimate_archive_library(
        "BTCUSDT",
        ["aggTrades"],
        [],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["partial_bytes"] == 400
    assert summary["remaining_bytes"] == 600
    assert summary["partial_files"] == 1


def test_existing_archive_requires_no_network_metadata_request(tmp_path):
    task = plan_archive_tasks(
        "BTCUSDT", ["aggTrades"], [], "2026-07-01", "2026-07-31", today=date(2026, 8, 21)
    )[0]
    final = tmp_path / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(b"x" * 777)

    def opener(*_args, **_kwargs):
        raise AssertionError("existing files should not require a network probe")

    summary = estimate_archive_library(
        "BTCUSDT",
        ["aggTrades"],
        [],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["present_bytes"] == 777
    assert summary["remaining_bytes"] == 0
    assert summary["present_files"] == 1


def test_missing_monthly_archive_estimates_daily_fallbacks(tmp_path):
    def opener(request, **_kwargs):
        url = request.full_url
        if "/monthly/" in url:
            raise HTTPError(url, 404, "missing", hdrs=None, fp=None)
        if "/daily/" in url:
            return Response(status=200, headers={"Content-Length": "100"})
        raise AssertionError(url)

    summary = estimate_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1h"],
        tmp_path,
        "2026-07-01",
        "2026-07-02",
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["remaining_bytes"] == 200
    assert summary["remaining_files"] == 2
    assert summary["unavailable_files"] == 0


def test_available_size_metadata_is_cached(tmp_path):
    calls = 0

    def opener(_request, **_kwargs):
        nonlocal calls
        calls += 1
        return Response(status=200, headers={"Content-Length": "1234"})

    kwargs = dict(
        symbols="BTCUSDT",
        datasets=["aggTrades"],
        intervals=[],
        root=tmp_path,
        start_date="2026-07-01",
        end_date="2026-07-31",
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )
    first = estimate_archive_library(**kwargs)
    second = estimate_archive_library(**kwargs)

    assert first["remaining_bytes"] == 1234
    assert second["remaining_bytes"] == 1234
    assert calls == 1
