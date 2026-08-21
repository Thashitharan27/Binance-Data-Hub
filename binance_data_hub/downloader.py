"""Binance Data Hub downloader API.

Version 6 keeps Binance's official archives, uses adaptive segmented transport,
records live/persistent performance telemetry, and can auto-tune the global
connection cap against Binance's CDN.
"""
from .archive_downloader import ArchiveTask, DownloadResult, plan_archive_tasks
from .benchmark import (
    DEFAULT_BENCHMARK_LEVELS,
    DEFAULT_BENCHMARK_SECONDS,
    benchmark_connections,
    recent_benchmark_history,
    recommend_connections,
)
from .fast_downloader import (
    DEFAULT_FILE_WORKERS,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SEGMENTS,
    MAX_CONNECTIONS,
    MAX_FILE_WORKERS,
    MAX_SEGMENTS,
    download_archive_library,
)
from .performance import recent_run_history

DEFAULT_WORKERS = DEFAULT_FILE_WORKERS
MAX_WORKERS = MAX_FILE_WORKERS

__all__ = [
    "DEFAULT_WORKERS",
    "MAX_WORKERS",
    "DEFAULT_MAX_CONNECTIONS",
    "MAX_CONNECTIONS",
    "DEFAULT_SEGMENTS",
    "MAX_SEGMENTS",
    "DEFAULT_BENCHMARK_LEVELS",
    "DEFAULT_BENCHMARK_SECONDS",
    "ArchiveTask",
    "DownloadResult",
    "download_archive_library",
    "plan_archive_tasks",
    "recent_run_history",
    "benchmark_connections",
    "recent_benchmark_history",
    "recommend_connections",
]
