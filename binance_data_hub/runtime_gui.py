from __future__ import annotations

import traceback

from PySide6.QtCore import QObject, QThread, Signal, Slot, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QFormLayout, QGroupBox, QLabel, QSpinBox,
)

from . import DATA_ROOT
from .catalog import DATASETS
from .downloader import DEFAULT_SEGMENTS
from .gui import _duration
from .responsive_gui import ResponsiveMainWindow
from .runtime_download import download_archive_library_runtime


class RuntimeWorker(QObject):
    status = Signal(str, int)
    telemetry = Signal(dict)
    calibration = Signal(str)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(
        self,
        symbols,
        datasets,
        intervals,
        start,
        end,
        connections,
        verify,
        auto_recalibrate,
        recalibration_minutes,
    ):
        super().__init__()
        self.symbols = symbols
        self.datasets = datasets
        self.intervals = intervals
        self.start = start
        self.end = end
        self.connections = connections
        self.verify = verify
        self.auto_recalibrate = auto_recalibrate
        self.recalibration_minutes = recalibration_minutes
        self.cancelled = False

    @Slot()
    def run(self):
        try:
            def progress(done, total, result):
                percent = min(99, int(done / max(1, total) * 100))
                interval = f" {result.task.interval}" if result.task.interval else ""
                transport = str(getattr(result, "transport", ""))
                mode = f" [{transport}]" if transport and transport != "existing" else ""
                self.status.emit(
                    f"{done:,}/{total:,} files — {result.task.symbol} {result.task.dataset}{interval} "
                    f"{result.task.key}: {result.status}{mode}",
                    percent,
                )

            result = download_archive_library_runtime(
                self.symbols,
                self.datasets,
                self.intervals,
                DATA_ROOT,
                self.start,
                self.end,
                workers=self.connections,
                max_connections=self.connections,
                segments=DEFAULT_SEGMENTS,
                verify=self.verify,
                progress=progress,
                telemetry=lambda snapshot: self.telemetry.emit(snapshot),
                cancelled=lambda: self.cancelled,
                auto_recalibrate=self.auto_recalibrate,
                recalibration_minutes=self.recalibration_minutes,
                recalibration_probe_seconds=20,
                auto_max_connections=32,
                recalibration_event=lambda message: self.calibration.emit(message),
            )
            self.status.emit("Archive collection finished.", 100)
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class RuntimeMainWindow(ResponsiveMainWindow):
    """Responsive Hub UI with optional hourly in-run connection optimization."""

    def __init__(self):
        super().__init__()

        scroll = self.centralWidget()
        content = scroll.widget()
        root_layout = content.layout()

        optimizer = QGroupBox("Long-run network optimization")
        optimizer_form = QFormLayout(optimizer)
        self.auto_recalibrate = QCheckBox("Auto calibrate connection cap during long downloads")
        self.auto_recalibrate.setChecked(True)
        self.auto_recalibrate.setToolTip(
            "Uses the real archive traffic once per interval. It briefly tests a higher cap up to 32 "
            "and keeps it only when sustained throughput improves materially."
        )
        self.recalibration_minutes = QSpinBox()
        self.recalibration_minutes.setRange(15, 180)
        self.recalibration_minutes.setValue(60)
        self.recalibration_minutes.setSuffix(" min")
        self.recalibration_minutes.setToolTip("60 minutes is recommended for long overnight downloads.")
        self.auto_optimizer_status = QLabel(
            "Starts from the selected Max HTTP connections. Every hour it can test the next higher cap "
            "using the download itself; repeated network errors cause a backoff. Maximum automatic cap: 32."
        )
        self.auto_optimizer_status.setWordWrap(True)
        optimizer_form.addRow("", self.auto_recalibrate)
        optimizer_form.addRow("Recheck interval", self.recalibration_minutes)
        optimizer_form.addRow("Status", self.auto_optimizer_status)

        # Collection request is item 1; put optimization immediately after it.
        root_layout.insertWidget(2, optimizer)

    def set_busy(self, busy):
        super().set_busy(busy)
        if hasattr(self, "auto_recalibrate"):
            self.auto_recalibrate.setEnabled(not busy)
            self.recalibration_minutes.setEnabled(not busy)

    def run_download(self):
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

        self.operation = "download"
        self.set_busy(True)
        self.progress.setValue(0)
        auto_text = (
            f"; hourly auto calibration every {self.recalibration_minutes.value()} min"
            if self.auto_recalibrate.isChecked() else ""
        )
        self.status.setText(f"Planning archive files{auto_text}...")
        self.reset_performance()
        if self.auto_recalibrate.isChecked():
            self.auto_optimizer_status.setText(
                f"Enabled — starting at {self.connections.value()} connections; "
                f"first automatic recheck in {self.recalibration_minutes.value()} minutes."
            )
        else:
            self.auto_optimizer_status.setText("Disabled for this run.")

        self.thread = QThread(self)
        self.worker = RuntimeWorker(
            symbols,
            datasets,
            intervals,
            self.start.text().strip() or None,
            self.end.text().strip() or None,
            self.connections.value(),
            self.verify.isChecked(),
            self.auto_recalibrate.isChecked(),
            self.recalibration_minutes.value(),
        )
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.status.connect(self.set_status)
        self.worker.telemetry.connect(self.set_telemetry)
        self.worker.calibration.connect(self.set_calibration_event)
        self.worker.finished.connect(self.done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    def set_telemetry(self, snapshot):
        super().set_telemetry(snapshot)
        if "connection_cap" in snapshot:
            self.perf_labels["connections"].setText(str(snapshot.get("connection_cap")))

    def set_calibration_event(self, message):
        self.auto_optimizer_status.setText(message)
        self.status.setText(message)

    def done(self, summary):
        super().done(summary)
        if summary.get("auto_recalibrate"):
            adjustments = summary.get("connection_adjustments", [])
            final_cap = int(summary.get("final_connection_cap", self.connections.value()))
            self.connections.setValue(final_cap)
            if adjustments:
                self.auto_optimizer_status.setText(
                    f"Run finished at {final_cap} connections after {len(adjustments)} automatic adjustment(s)."
                )
            else:
                self.auto_optimizer_status.setText(
                    f"Run finished at {final_cap} connections; no automatic cap change was needed."
                )


def main():
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    window = RuntimeMainWindow()
    app._main_window = window
    window.show()
    window.raise_()
    window.activateWindow()
    return app.exec()
