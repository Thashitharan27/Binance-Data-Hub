"""Binance Data Hub downloader API.

Version 3 stores official Binance archives directly for maximum collection
speed. See :mod:`binance_data_hub.archive_downloader`.
"""
from .archive_downloader import (
    DEFAULT_WORKERS,
    MAX_WORKERS,
    ArchiveTask,
    DownloadResult,
    download_archive_library,
    plan_archive_tasks,
)

__all__ = [
    "DEFAULT_WORKERS",
    "MAX_WORKERS",
    "ArchiveTask",
    "DownloadResult",
    "download_archive_library",
    "plan_archive_tasks",
]
