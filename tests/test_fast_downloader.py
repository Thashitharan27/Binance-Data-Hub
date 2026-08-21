import io
import zipfile
from datetime import date
from urllib.error import HTTPError

from binance_data_hub.fast_downloader import download_archive_library


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


def zip_payload(size=4096):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data.csv", "x" * size)
    return buffer.getvalue()


def range_opener(payload, *, advertise_ranges=True):
    calls = []

    def opener(request, **_kwargs):
        calls.append((request.get_method(), request.get_header("Range")))
        if request.get_method() == "HEAD":
            headers = {"Content-Length": str(len(payload))}
            if advertise_ranges:
                headers["Accept-Ranges"] = "bytes"
            return Response(status=200, headers=headers)

        value = request.get_header("Range")
        if value and advertise_ranges:
            bounds = value.removeprefix("bytes=").split("-", 1)
            start = int(bounds[0])
            end = int(bounds[1]) if bounds[1] else len(payload) - 1
            return Response(
                payload[start : end + 1],
                status=206,
                headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}"},
            )
        return Response(payload, status=200)

    return opener, calls


def test_large_monthly_archive_uses_segmented_ranges(tmp_path):
    payload = zip_payload(20_000)
    opener, calls = range_opener(payload, advertise_ranges=True)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=2,
        max_connections=4,
        segments=4,
        segment_threshold_mb=0.00001,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["counts"]["downloaded"] == 1
    assert summary["segmented_files"] == 1
    result = summary["results"][0]
    assert result.transport.startswith("segmented-")
    assert any(method == "HEAD" for method, _ in calls)
    assert sum(1 for _, byte_range in calls if byte_range) >= 2
    stored = tmp_path / result.task.relative_path
    assert stored.read_bytes() == payload
    assert zipfile.is_zipfile(stored)


def test_range_unsupported_falls_back_to_single_stream(tmp_path):
    payload = zip_payload(20_000)
    opener, calls = range_opener(payload, advertise_ranges=False)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=2,
        max_connections=4,
        segments=4,
        segment_threshold_mb=0.00001,
        opener=opener,
        today=date(2026, 8, 21),
    )

    result = summary["results"][0]
    assert result.status == "downloaded"
    assert result.transport == "single"
    assert any(method == "HEAD" for method, _ in calls)
    assert not any(byte_range for _, byte_range in calls)
    assert (tmp_path / result.task.relative_path).read_bytes() == payload


def test_daily_metrics_skip_head_probe_and_use_parallel_file_transport(tmp_path):
    payload = zip_payload(1000)
    opener, calls = range_opener(payload, advertise_ranges=True)

    summary = download_archive_library(
        "BTCUSDT",
        ["metrics"],
        [],
        tmp_path,
        "2026-08-19",
        "2026-08-20",
        workers=2,
        max_connections=2,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["counts"]["downloaded"] == 2
    assert summary["segmented_files"] == 0
    assert all(method != "HEAD" for method, _ in calls)


def test_fast_transport_preserves_404_as_missing(tmp_path):
    def opener(request, **_kwargs):
        raise HTTPError(request.full_url, 404, "missing", {}, None)

    summary = download_archive_library(
        "BTCUSDT",
        ["metrics"],
        [],
        tmp_path,
        "2026-08-20",
        "2026-08-20",
        workers=1,
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )
    assert summary["counts"]["missing"] == 1
