from __future__ import annotations

import traceback

from PySide6.QtCore import QObject, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QApplication, QFrame, QGroupBox, QHBoxLayout, QLabel, QPushButton,
    QScrollArea, QTableWidget, QTableWidgetItem, QVBoxLayout,
)

from . import DATA_ROOT
from .catalog import DATASETS
from .estimator import estimate_archive_library
from .gui import MainWindow, _duration, _size


class EstimateWorker(QObject):
    status = Signal(str, int)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, symbols, datasets, intervals, start, end, connections):
        super().__init__()
        self.symbols = symbols
        self.datasets = datasets
        self.intervals = intervals
        self.start = start
        self.end = end
        self.connections = connections
        self.cancelled = False

    @Slot()
    def run(self):
        try:
            def progress(done, total, task, _entries):
                percent = min(99, int(done / max(1, total) * 100))
                interval = f" {task.interval}" if task.interval else ""
                self.status.emit(
                    f"Estimating {done:,}/{total:,} planned archives — "
                    f"{task.symbol} {task.dataset}{interval} {task.key}",
                    percent,
                )

            result = estimate_archive_library(
                self.symbols,
                self.datasets,
                self.intervals,
                DATA_ROOT,
                self.start,
                self.end,
                max_connections=self.connections,
                progress=progress,
                cancelled=lambda: self.cancelled,
            )
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class ResponsiveMainWindow(MainWindow):
    """Main window wrapper that keeps the growing Hub usable on smaller screens."""

    def __init__(self):
        super().__init__()

        content = self.centralWidget()
        root_layout = content.layout()

        self.estimate_btn = QPushButton("Estimate Size / Time")
        self.estimate_btn.setToolTip(
            "Inspect Binance archive metadata without downloading the ZIP bodies. "
            "Shows remaining storage, free disk and an ETA using recent measured speed."
        )
        self.estimate_btn.clicked.connect(self.run_estimate)

        # The base GUI's top-level item 4 is its action-button row. Keep the
        # estimator beside Collect and Auto Tune when that structure is present.
        action_item = root_layout.itemAt(4)
        action_layout = action_item.layout() if action_item is not None else None
        if action_layout is not None:
            action_layout.insertWidget(2, self.estimate_btn)
        else:
            fallback_actions = QHBoxLayout()
            fallback_actions.addWidget(self.estimate_btn)
            fallback_actions.addStretch(1)
            root_layout.insertLayout(5, fallback_actions)

        estimate_box = QGroupBox("Download estimate")
        estimate_layout = QVBoxLayout(estimate_box)
        self.estimate_summary = QLabel(
            "Click Estimate Size / Time after choosing symbols, dates, datasets and intervals. "
            "The estimator checks file metadata only; it does not download archive bodies."
        )
        self.estimate_summary.setWordWrap(True)
        estimate_layout.addWidget(self.estimate_summary)
        self.estimate_table = QTableWidget(0, 5)
        self.estimate_table.setHorizontalHeaderLabels(
            ["Dataset", "Resolved files", "Files needed", "Remaining", "Unavailable / unknown"]
        )
        self.estimate_table.setMaximumHeight(140)
        estimate_layout.addWidget(self.estimate_table)

        # Insert after progress/status and before Auto Tune results.
        root_layout.insertWidget(7, estimate_box)

        content = self.takeCentralWidget()
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidget(content)
        self.setCentralWidget(scroll)

        # Keep data tables compact; each table has its own scrollbar when needed.
        self.benchmark_table.setMaximumHeight(135)
        self.estimate_table.setMaximumHeight(135)
        self.table.setMaximumHeight(125)
        self.history.setMinimumHeight(150)
        self.history.setMaximumHeight(190)

        # Size to the actual Windows work area rather than assuming a 1000 px
        # vertical desktop. This accounts for the taskbar and display scaling.
        screen = QApplication.primaryScreen()
        if screen is not None:
            area = screen.availableGeometry()
            width = min(1120, max(760, int(area.width() * 0.92)))
            height = min(820, max(600, int(area.height() * 0.88)))
            width = min(width, area.width())
            height = min(height, area.height())
            self.resize(width, height)
            self.move(
                area.x() + max(0, (area.width() - width) // 2),
                area.y() + max(0, (area.height() - height) // 2),
            )
        else:
            self.resize(1100, 760)

    def set_busy(self, busy):
        super().set_busy(busy)
        if hasattr(self, "estimate_btn"):
            self.estimate_btn.setEnabled(not busy)

    def _selected_request(self):
        symbols = [
            item.strip().upper().replace("/", "")
            for item in self.symbol.text().replace(",", " ").split()
            if item.strip()
        ]
        datasets = [key for key, box in self.dataset_checks.items() if box.isChecked()]
        intervals = [key for key, box in self.interval_checks.items() if box.isChecked()]
        needs_intervals = any(DATASETS[key].intervalled for key in datasets)
        if not symbols or not datasets or (needs_intervals and not intervals):
            return None
        return symbols, datasets, intervals

    def run_estimate(self):
        selection = self._selected_request()
        if selection is None:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.warning(
                self,
                "Choose data",
                "Enter one or more symbols, choose datasets, and select at least one interval for kline datasets.",
            )
            return
        symbols, datasets, intervals = selection

        self.operation = "estimate"
        self.set_busy(True)
        self.progress.setValue(0)
        self.status.setText(
            "Estimating archive sizes from Binance metadata. The first estimate may take time; "
            "historical sizes are cached for later estimates."
        )

        self.thread = QThread(self)
        self.worker = EstimateWorker(
            symbols,
            datasets,
            intervals,
            self.start.text().strip() or None,
            self.end.text().strip() or None,
            self.connections.value(),
        )
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.status.connect(self.set_status)
        self.worker.finished.connect(self.estimate_done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    def cancel(self):
        if self.operation == "estimate" and self.worker:
            self.worker.cancelled = True
            self.status.setText("Stopping size estimate after active metadata requests finish...")
            return
        super().cancel()

    def estimate_done(self, summary):
        self.set_busy(False)
        self.operation = None
        if summary.get("cancelled"):
            self.status.setText("Download-size estimate cancelled.")
            return

        self.progress.setValue(100)
        remaining = int(summary.get("remaining_bytes", 0))
        free = int(summary.get("free_bytes", 0))
        speed = float(summary.get("speed_mbps", 0) or 0)
        eta = summary.get("eta_seconds")
        unknown = int(summary.get("unknown_files", 0))
        unavailable = int(summary.get("unavailable_files", 0))
        partial = int(summary.get("partial_bytes", 0))
        present = int(summary.get("present_bytes", 0))

        eta_text = f"~{_duration(eta)}" if eta is not None else "unavailable until a measured speed exists"
        disk_text = "enough free disk" if summary.get("enough_disk") else "NOT ENOUGH FREE DISK"
        speed_text = (
            f"{speed:.2f} Mbps ({summary.get('speed_source', '')})"
            if speed > 0 else summary.get("speed_source", "no measured speed yet")
        )
        warning = ""
        if unknown:
            warning = f" {unknown:,} archive sizes could not be resolved, so the true total may be higher."

        self.estimate_summary.setText(
            f"Remaining: {_size(remaining)} across {summary.get('remaining_files', 0):,} files. "
            f"Already complete: {_size(present)}; resumable partial data: {_size(partial)}. "
            f"Free disk: {_size(free)} ({disk_text}). "
            f"ETA: {eta_text} using {speed_text}. "
            f"Unavailable archives: {unavailable:,}.{warning}"
        )

        rows = sorted(summary.get("by_dataset", {}).items())
        self.estimate_table.setRowCount(len(rows))
        for row_index, (dataset, item) in enumerate(rows):
            unresolved = int(item.get("unavailable_files", 0)) + int(item.get("unknown_files", 0))
            values = [
                dataset,
                item.get("files", 0),
                item.get("needed_files", 0),
                _size(item.get("remaining_bytes", 0)),
                unresolved,
            ]
            for col, value in enumerate(values):
                self.estimate_table.setItem(row_index, col, QTableWidgetItem(str(value)))
        self.estimate_table.resizeColumnsToContents()

        self.status.setText(
            f"Estimate complete — {_size(remaining)} remaining; {_size(free)} free; "
            f"ETA {eta_text}."
        )
        QApplication.beep()


def main():
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    window = ResponsiveMainWindow()
    app._main_window = window
    window.show()
    window.raise_()
    window.activateWindow()
    return app.exec()
