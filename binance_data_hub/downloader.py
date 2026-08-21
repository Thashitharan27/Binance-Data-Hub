"""Binance Data Hub downloader API.

Version 4 keeps Binance's official archives and uses an adaptive transport:
small files use one resumable stream, while large monthly ZIPs can use multiple
HTTP byte-range streams under a global connection cap.
"""
from .archive_downloader import ArchiveTask, DownloadResult, plan_archive_tasks
from .fast_downloader import (
    DEFAULT_FILE_WORKERS,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SEGMENTS,
    MAX_CONNECTIONS,
    MAX_FILE_WORKERS,
    MAX_SEGMENTS,
    download_archive_library,
)

DEFAULT_WORKERS = DEFAULT_FILE_WORKERS
MAX_WORKERS = MAX_FILE_WORKERS

__all__ = [
    "DEFAULT_WORKERS",
    "MAX_WORKERS",
    "DEFAULT_MAX_CONNECTIONS",
    "MAX_CONNECTIONS",
    "DEFAULT_SEGMENTS",
    "MAX_SEGMENTS",
    "ArchiveTask",
    "DownloadResult",
    "download_archive_library",
    "plan_archive_tasks",
]
