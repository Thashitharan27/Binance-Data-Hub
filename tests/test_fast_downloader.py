import io
import zipfile
from datetime import date
from urllib.error import HTTPError

from binance_data_hub.archive_downloader import plan_archive_tasks
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


def monthly_1m_task():
    return plan_archive_tasks(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        "2026-07-01",
        "2026-07-31",
        today=date(2026, 8, 21),
    )[0]


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


def test_complete_part_is_promoted_without_redownload(tmp_path):
    payload = zip_payload(20_000)
    task = monthly_1m_task()
    final = tmp_path / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(f"{final.name}.part")
    part.write_bytes(payload)

    def no_network(*_args, **_kwargs):
        raise AssertionError("A complete valid .part must not be downloaded again")

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=1,
        max_connections=1,
        opener=no_network,
        today=date(2026, 8, 21),
    )

    assert summary["recovered_parts"] == 1
    assert summary["counts"]["skipped"] == 1
    assert summary["results"][0].transport == "recovered-part"
    assert final.read_bytes() == payload
    assert not part.exists()


def test_incomplete_part_resumes_before_segmenting(tmp_path):
    payload = zip_payload(20_000)
    task = monthly_1m_task()
    final = tmp_path / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(f"{final.name}.part")
    prefix_size = max(1, len(payload) // 3)
    part.write_bytes(payload[:prefix_size])
    opener, calls = range_opener(payload, advertise_ranges=True)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=1,
        max_connections=2,
        segments=4,
        segment_threshold_mb=0.00001,
        opener=opener,
        today=date(2026, 8, 21),
    )

    result = summary["results"][0]
    assert result.status == "downloaded"
    assert result.transport == "single"
    assert ("GET", f"bytes={prefix_size}-") in calls
    assert not any(method == "HEAD" for method, _ in calls)
    assert final.read_bytes() == payload
    assert not part.exists()


def test_corrupt_full_part_restarts_cleanly_after_range_416(tmp_path):
    payload = zip_payload(20_000)
    task = monthly_1m_task()
    final = tmp_path / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(f"{final.name}.part")
    part.write_bytes(b"!" * len(payload))
    calls = []

    def opener(request, **_kwargs):
        byte_range = request.get_header("Range")
        calls.append((request.get_method(), byte_range))
        if byte_range:
            start = int(byte_range.removeprefix("bytes=").split("-", 1)[0])
            if start >= len(payload):
                raise HTTPError(request.full_url, 416, "range not satisfiable", {}, None)
            return Response(payload[start:], status=206)
        return Response(payload, status=200)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=1,
        max_connections=1,
        segments=4,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["counts"]["downloaded"] == 1
    assert calls[0] == ("GET", f"bytes={len(payload)}-")
    assert ("GET", None) in calls[1:]
    assert final.read_bytes() == payload
    assert not part.exists()


def test_corrupt_published_zip_is_not_skipped(tmp_path):
    payload = zip_payload(20_000)
    task = monthly_1m_task()
    final = tmp_path / task.relative_path
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(b"not-a-zip")
    opener, _calls = range_opener(payload, advertise_ranges=False)

    summary = download_archive_library(
        "BTCUSDT",
        ["klines"],
        ["1m"],
        tmp_path,
        "2026-07-01",
        "2026-07-31",
        workers=1,
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )

    assert summary["counts"]["downloaded"] == 1
    assert summary["counts"]["skipped"] == 0
    assert final.read_bytes() == payload
    assert zipfile.is_zipfile(final)
