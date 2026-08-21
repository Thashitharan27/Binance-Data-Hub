import io
import zipfile
from datetime import date

import binance_data_hub.performance as performance_module
from binance_data_hub.downloader import download_archive_library, recent_run_history
from binance_data_hub.performance import TransferMeter


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
    assert perf["current_window_seconds"] == 5.0
    assert perf["peak_window_seconds"] == 10.0
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


def test_short_buffered_burst_does_not_become_current_or_peak_speed(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(performance_module.time, "monotonic", lambda: clock[0])
    meter = TransferMeter(emit_interval=999)

    clock[0] = 0.1
    meter.add_bytes(20 * 1024 * 1024)
    snapshot = meter.finish()

    assert snapshot["average_bps"] > 0
    assert snapshot["current_bps"] == 0
    assert snapshot["peak_bps"] == 0


def test_peak_speed_requires_sustained_multi_second_transfer(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(performance_module.time, "monotonic", lambda: clock[0])
    meter = TransferMeter(emit_interval=999)

    clock[0] = 2.0
    meter.add_bytes(2 * 1024 * 1024)
    early = meter.finish()
    assert early["current_bps"] > 0
    assert early["peak_bps"] == 0

    clock[0] = 5.0
    meter.add_bytes(3 * 1024 * 1024)
    sustained = meter.finish()
    assert sustained["current_bps"] > 0
    assert sustained["peak_bps"] > 0
    assert sustained["peak_observed_span_seconds"] >= 5.0
