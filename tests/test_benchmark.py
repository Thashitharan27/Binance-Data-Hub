import io
from datetime import date, datetime, timezone

from binance_data_hub.benchmark import (
    BenchmarkSource,
    _record,
    find_benchmark_source,
    recent_benchmark_history,
    recommend_connections,
)


class Response(io.BytesIO):
    def __init__(self, payload=b"x", status=206, headers=None):
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def getcode(self):
        return self.status


def test_recommendation_uses_smallest_connection_count_within_five_percent_of_best():
    result = recommend_connections([
        {"connections": 4, "average_mbps": 4.50, "network_bytes": 1, "errors": 0},
        {"connections": 8, "average_mbps": 4.90, "network_bytes": 1, "errors": 0},
        {"connections": 16, "average_mbps": 5.00, "network_bytes": 1, "errors": 0},
        {"connections": 24, "average_mbps": 4.98, "network_bytes": 1, "errors": 0},
        {"connections": 32, "average_mbps": 4.92, "network_bytes": 1, "errors": 0},
    ])
    assert result["best_connections"] == 16
    assert result["recommended_connections"] == 8
    assert result["efficiency_pct"] == 98.0


def test_recommendation_moves_higher_when_lower_setting_is_materially_slower():
    result = recommend_connections([
        {"connections": 4, "average_mbps": 5.0, "network_bytes": 1, "errors": 0},
        {"connections": 8, "average_mbps": 7.0, "network_bytes": 1, "errors": 0},
        {"connections": 16, "average_mbps": 10.0, "network_bytes": 1, "errors": 0},
        {"connections": 24, "average_mbps": 10.2, "network_bytes": 1, "errors": 0},
    ])
    assert result["recommended_connections"] == 16
    assert result["best_connections"] == 24


def test_find_benchmark_source_uses_range_probe():
    size = 200 * 1024 * 1024

    def opener(request, **_kwargs):
        return Response(b"x", 206, {"Content-Range": f"bytes 0-0/{size}"})

    source = find_benchmark_source("ETHUSDT", opener=opener, today=date(2026, 8, 21))
    assert source.symbol == "ETHUSDT"
    assert source.size == size
    assert "aggTrades" in source.url
    assert source.key == "2026-07"


def test_benchmark_history_persists_recommendation(tmp_path):
    source = BenchmarkSource(
        url="https://data.binance.vision/example.zip",
        size=100_000_000,
        symbol="BTCUSDT",
        key="2026-07",
    )
    results = [
        {"connections": 4, "average_mbps": 5.0, "network_bytes": 1000, "errors": 0},
        {"connections": 8, "average_mbps": 5.2, "network_bytes": 1000, "errors": 0},
    ]
    recommendation = recommend_connections(results)
    _record(tmp_path, datetime.now(timezone.utc), source, 15, (4, 8), recommendation, results)
    rows = recent_benchmark_history(tmp_path, 1)
    assert len(rows) == 1
    assert rows[0]["recommended_connections"] == 4
    assert rows[0]["best_connections"] == 8
