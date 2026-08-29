from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from binance_data_hub.runtime_gui import RuntimeMainWindow


def _app():
    return QApplication.instance() or QApplication([])


def test_repair_result_is_rendered_only_after_thread_finishes(monkeypatch):
    _app()
    window = RuntimeMainWindow()
    rendered = []

    monkeypatch.setattr(window, "repair_done", lambda summary: rendered.append(summary))
    summary = {"mode": "scan", "cancelled": False, "scan": {}}

    window._repair_worker_succeeded(summary)

    assert rendered == []
    assert window._pending_repair_result is summary

    # Simulate the QThread.finished cleanup stage. GUI rendering should happen
    # only here, after the worker thread is no longer running.
    window._repair_thread_finished()

    assert rendered == [summary]
    assert window._pending_repair_result is None
    assert window._pending_repair_error is None
    assert window.thread is None
    assert window.worker is None
    window.close()
