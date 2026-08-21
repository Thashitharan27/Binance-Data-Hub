import csv
import hashlib
import io
import json
import zipfile
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

from binance_data_hub.downloader import download_klines


class Response(io.BytesIO):
    headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def kline(timestamp, close="1.5"):
    return [timestamp, "1", "2", "0.5", close, "10"] + [0] * 6


def archive_payload(rows, name="ETHUSDT-1m-2020-01.csv"):
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(rows)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, output.getvalue())
    return buffer.getvalue()


def test_rest_only_download_remains_compatible(tmp_path):
    rows = [kline(1577836800000)]
    path = tmp_path / "BTCUSDT_1m.csv"
    result = download_klines(
        "BTC/USDT",
        "1m",
        path,
        "2020-01-01",
        "2020-01-01",
        opener=lambda *_a, **_k: Response(json.dumps(rows).encode()),
    )
    assert result["total"] == 1
    assert result["archives"] == 0
    assert path.read_text().splitlines()[0] == "timestamp,open,high,low,close,volume"


def test_automatic_archive_download_verifies_and_publishes(tmp_path):
    rows = [kline(1577836800000), kline(1577836860000, "1.6")]
    payload = archive_payload(rows)

    def rest_opener(request, **_kwargs):
        return Response(json.dumps([rows[0]]).encode())

    def archive_opener(request, **_kwargs):
        if request.full_url.endswith(".CHECKSUM"):
            return Response(f"{hashlib.sha256(payload).hexdigest()}  archive.zip\n".encode())
        return Response(payload)

    path = tmp_path / "ETHUSDT_1m.csv"
    result = download_klines(
        "ETHUSDT",
        "1m",
        path,
        "2020-01-01",
        "2020-01-31",
        opener=rest_opener,
        archive_opener=archive_opener,
        use_archives=True,
    )
    assert result == {
        "path": str(path.resolve()),
        "added": 2,
        "total": 2,
        "gaps": 0,
        "archives": 1,
        "rest_segments": 0,
    }
    assert path.read_text().splitlines()[1:] == [
        "1577836800000,1,2,0.5,1.5,10",
        "1577836860000,1,2,0.5,1.6,10",
    ]
    assert not path.with_name(f".{path.name}.parts").exists()


def test_missing_archive_automatically_falls_back_to_rest(tmp_path):
    rows = [kline(1577836800000), kline(1577836860000)]

    def rest_opener(request, **_kwargs):
        limit = parse_qs(urlparse(request.full_url).query)["limit"][0]
        selected = [rows[0]] if limit == "1" else rows
        return Response(json.dumps(selected).encode())

    def missing_archive(request, **_kwargs):
        raise HTTPError(request.full_url, 404, "missing", {}, None)

    path = tmp_path / "ETHUSDT_1m.csv"
    result = download_klines(
        "ETHUSDT",
        "1m",
        path,
        "2020-01-01",
        "2020-01-31",
        opener=rest_opener,
        archive_opener=missing_archive,
        use_archives=True,
    )
    assert result["archives"] == 0
    assert result["rest_segments"] == 1
    assert result["total"] == 2


def test_pause_keeps_verified_parts_and_next_run_reuses_them(tmp_path):
    rows = [kline(1577836800000), kline(1577836860000)]
    payload = archive_payload(rows)
    archive_requests = []
    paused = {"value": False}

    def rest_opener(_request, **_kwargs):
        return Response(json.dumps([rows[0]]).encode())

    def archive_opener(request, **_kwargs):
        archive_requests.append(request.full_url)
        if request.full_url.endswith(".CHECKSUM"):
            return Response(f"{hashlib.sha256(payload).hexdigest()}  archive.zip\n".encode())
        return Response(payload)

    def progress(_count, detail):
        if "verified archive" in detail:
            paused["value"] = True

    path = tmp_path / "ETHUSDT_1m.csv"
    with pytest.raises(InterruptedError):
        download_klines(
            "ETHUSDT",
            "1m",
            path,
            "2020-01-01",
            "2020-01-31",
            opener=rest_opener,
            archive_opener=archive_opener,
            use_archives=True,
            progress=progress,
            cancelled=lambda: paused["value"],
        )
    assert not path.exists()
    assert path.with_name(f".{path.name}.parts").exists()
    first_request_count = len(archive_requests)

    paused["value"] = False
    result = download_klines(
        "ETHUSDT",
        "1m",
        path,
        "2020-01-01",
        "2020-01-31",
        opener=rest_opener,
        archive_opener=archive_opener,
        use_archives=True,
    )
    assert result["total"] == 2
    assert len(archive_requests) == first_request_count
    assert not path.with_name(f".{path.name}.parts").exists()


def test_existing_legacy_checkpoint_is_merged_on_upgrade(tmp_path):
    path = tmp_path / "ETHUSDT_1m.csv"
    checkpoint = path.with_name(f".{path.name}.download")
    header = "timestamp,open,high,low,close,volume\n"
    path.write_text(header + "1577836800000,1,2,0.5,1.5,10\n", encoding="utf-8")
    checkpoint.write_text(header + "1577836860000,1,2,0.5,1.6,10\n", encoding="utf-8")

    result = download_klines(
        "ETHUSDT",
        "1m",
        path,
        "2020-01-01",
        "2020-01-01 00:01:00",
        opener=lambda *_a, **_k: Response(b"[]"),
    )
    assert result["total"] == 2
    assert result["added"] == 1
    assert not checkpoint.exists()
    assert path.read_text(encoding="utf-8").splitlines()[-1].startswith("1577836860000,")


def test_progress_state_reports_completed_and_expected_candles(tmp_path):
    rows = [kline(1577836800000), kline(1577836860000)]
    updates = []

    def rest_opener(request, **_kwargs):
        return Response(json.dumps(rows).encode())

    path = tmp_path / "BTCUSDT_1m.csv"
    download_klines(
        "BTCUSDT",
        "1m",
        path,
        "2020-01-01 00:00:00",
        "2020-01-01 00:01:00",
        opener=rest_opener,
        progress_state=lambda completed, total, detail: updates.append((completed, total, detail)),
    )
    assert updates
    assert updates[-1][0:2] == (2, 2)
