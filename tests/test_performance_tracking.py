import io
import zipfile
from datetime import date

from binance_data_hub.downloader import download_archive_library, recent_run_history


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


def zip_payload():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BTCUSDT-metrics-2026-08-20.csv", "create_time,sum_open_interest\n1,2\n")
    return buffer.getvalue()


def test_collection_reports_live_performance_and_saves_history(tmp_path):
    payload = zip_payload()
    telemetry = []

    def opener(_request, **_kwargs):
        return Response(payload)

    summary = download_archive_library(
        "BTCUSDT",
        ["metrics"],
        [],
        tmp_path,
        "2026-08-20",
        "2026-08-20",
        workers=1,
        max_connections=1,
        telemetry=telemetry.append,
        opener=opener,
        today=date(2026, 8, 21),
    )

    perf = summary["performance"]
    assert summary["counts"]["downloaded"] == 1
    assert perf["network_bytes"] == len(payload)
    assert perf["elapsed_seconds"] > 0
    assert perf["average_bps"] >= 0
    assert perf["peak_bps"] >= 0
    assert perf["completed_files"] == 1
    assert telemetry

    history = recent_run_history(tmp_path)
    assert len(history) == 1
    assert history[0]["max_connections"] == 1
    assert history[0]["downloaded_files"] == 1
    assert history[0]["network_bytes"] == len(payload)


def test_skipped_existing_archive_records_zero_network_bytes(tmp_path):
    payload = zip_payload()

    def opener(_request, **_kwargs):
        return Response(payload)

    kwargs = dict(
        symbols="BTCUSDT",
        datasets=["metrics"],
        intervals=[],
        root=tmp_path,
        start_date="2026-08-20",
        end_date="2026-08-20",
        workers=1,
        max_connections=1,
        opener=opener,
        today=date(2026, 8, 21),
    )
    first = download_archive_library(**kwargs)
    second = download_archive_library(**kwargs)

    assert first["performance"]["network_bytes"] == len(payload)
    assert second["counts"]["skipped"] == 1
    assert second["performance"]["network_bytes"] == 0
    history = recent_run_history(tmp_path)
    assert len(history) == 2
    assert history[0]["network_bytes"] == 0
