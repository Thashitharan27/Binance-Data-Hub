from __future__ import annotations

import traceback

from PySide6.QtCore import QObject, QThread, Signal, Slot, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QFormLayout, QGridLayout, QGroupBox,
    QHBoxLayout, QLabel, QLineEdit, QMainWindow, QMessageBox, QPushButton,
    QProgressBar, QSpinBox, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget,
)

from . import DATA_ROOT
from .downloader import DEFAULT_MAX_CONNECTIONS, DEFAULT_SEGMENTS, download_archive_library
from .catalog import DATASETS, DEFAULT_INTERVALS, INTERVALS, research_core_keys


class Worker(QObject):
    status = Signal(str, int)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, symbols, datasets, intervals, start, end, connections, verify):
        super().__init__()
        self.symbols = symbols
        self.datasets = datasets
        self.intervals = intervals
        self.start = start
        self.end = end
        self.connections = connections
        self.verify = verify
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

            result = download_archive_library(
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
                cancelled=lambda: self.cancelled,
            )
            self.status.emit("Archive collection finished.", 100)
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Binance USD-M Futures Data Hub — Adaptive High-Speed Collector")
        self.resize(1100, 800)
        self.thread = None
        self.worker = None

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        intro = QLabel(
            "Speed-first collector for Binance's public USD-M Futures archive. Small ZIPs download in parallel; "
            "large monthly ZIPs automatically use multiple byte-range streams when Binance supports them. "
            "Official ZIP files are kept intact, avoiding CSV extraction and merge work during collection."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        request = QGroupBox("Collection request")
        form = QFormLayout(request)
        self.symbol = QLineEdit("BTCUSDT")
        self.symbol.setPlaceholderText("BTCUSDT  ETHUSDT  SOLUSDT  (comma or space separated)")
        self.start = QLineEdit("2020-01-01")
        self.end = QLineEdit()
        self.end.setPlaceholderText("Yesterday / latest published daily archive")
        self.connections = QSpinBox()
        self.connections.setRange(1, 64)
        self.connections.setValue(DEFAULT_MAX_CONNECTIONS)
        self.connections.setToolTip(
            "Global cap for active Binance HTTP connections. 32 is the recommended default. "
            "Small files use one connection; large monthly ZIPs can use up to four connections each."
        )
        self.verify = QCheckBox("Verify Binance SHA-256 checksums (extra disk read; safer, slightly slower)")
        self.output = QLineEdit(str(DATA_ROOT))
        self.output.setReadOnly(True)
        form.addRow("USD-M symbols", self.symbol)
        form.addRow("Start date", self.start)
        form.addRow("End date", self.end)
        form.addRow("Max HTTP connections", self.connections)
        form.addRow("", self.verify)
        form.addRow("Data lake", self.output)
        speed_note = QLabel(
            "Adaptive speed mode: up to 32 files can progress together by default. Large monthly 1m/3m/5m, "
            "trade and order-book archives are automatically split into up to four resumable byte ranges."
        )
        speed_note.setWordWrap(True)
        form.addRow("Speed mode", speed_note)
        layout.addWidget(request)

        datasets = QGroupBox("Datasets")
        datasets_layout = QGridLayout(datasets)
        self.dataset_checks = {}
        for index, (key, spec) in enumerate(DATASETS.items()):
            box = QCheckBox(spec.label)
            box.setToolTip(spec.note)
            box.setChecked(spec.research_core)
            self.dataset_checks[key] = box
            datasets_layout.addWidget(box, index // 2, index % 2)
        preset_row = QHBoxLayout()
        self.core_btn = QPushButton("Research Core")
        self.all_btn = QPushButton("Select Everything")
        self.none_btn = QPushButton("Clear")
        self.core_btn.clicked.connect(self.select_core)
        self.all_btn.clicked.connect(lambda: self.select_datasets(DATASETS.keys()))
        self.none_btn.clicked.connect(lambda: self.select_datasets([]))
        preset_row.addWidget(self.core_btn)
        preset_row.addWidget(self.all_btn)
        preset_row.addWidget(self.none_btn)
        preset_row.addStretch(1)
        datasets_layout.addLayout(preset_row, (len(DATASETS) + 1) // 2, 0, 1, 2)
        layout.addWidget(datasets)

        intervals_box = QGroupBox("Intervals for kline datasets")
        intervals_layout = QGridLayout(intervals_box)
        self.interval_checks = {}
        for index, interval in enumerate(INTERVALS):
            box = QCheckBox(interval)
            box.setChecked(interval in DEFAULT_INTERVALS)
            self.interval_checks[interval] = box
            intervals_layout.addWidget(box, index // 8, index % 8)
        layout.addWidget(intervals_box)

        actions = QHBoxLayout()
        self.run_btn = QPushButton("Collect / Update Archives")
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setEnabled(False)
        open_btn = QPushButton("Open Data Lake")
        self.run_btn.clicked.connect(self.run_download)
        self.cancel_btn.clicked.connect(self.cancel)
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(str(DATA_ROOT))))
        actions.addWidget(self.run_btn)
        actions.addWidget(self.cancel_btn)
        actions.addWidget(open_btn)
        actions.addStretch(1)
        layout.addLayout(actions)

        self.progress = QProgressBar()
        self.status = QLabel("Ready.")
        layout.addWidget(self.progress)
        layout.addWidget(self.status)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Dataset", "Downloaded", "Skipped", "Missing", "Failed"])
        layout.addWidget(self.table, 1)

    def select_datasets(self, keys):
        selected = set(keys)
        for key, box in self.dataset_checks.items():
            box.setChecked(key in selected)

    def select_core(self):
        self.select_datasets(research_core_keys())

    def run_download(self):
        symbols = [item.strip().upper().replace("/", "") for item in self.symbol.text().replace(",", " ").split() if item.strip()]
        datasets = [key for key, box in self.dataset_checks.items() if box.isChecked()]
        intervals = [key for key, box in self.interval_checks.items() if box.isChecked()]
        needs_intervals = any(DATASETS[key].intervalled for key in datasets)
        if not symbols or not datasets or (needs_intervals and not intervals):
            QMessageBox.warning(self, "Choose data", "Enter one or more symbols, choose datasets, and select at least one interval for kline datasets.")
            return

        self.run_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.progress.setValue(0)
        self.status.setText("Planning archive files...")

        self.thread = QThread(self)
        self.worker = Worker(
            symbols,
            datasets,
            intervals,
            self.start.text().strip() or None,
            self.end.text().strip() or None,
            self.connections.value(),
            self.verify.isChecked(),
        )
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.status.connect(self.set_status)
        self.worker.finished.connect(self.done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    def set_status(self, text, percent):
        self.status.setText(text)
        self.progress.setValue(percent)

    def cancel(self):
        if self.worker:
            self.worker.cancelled = True
            self.status.setText("Cancelling active downloads; completed ZIPs and resumable parts will be kept.")

    def done(self, summary):
        self.run_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.progress.setValue(100)
        self.render_summary(summary)
        counts = summary["counts"]
        gb = summary["bytes_downloaded"] / 1024 / 1024 / 1024
        segmented = summary.get("segmented_files", 0)
        self.status.setText(
            f"Finished: {counts['downloaded']} downloaded ({segmented} segmented), "
            f"{counts['skipped']} already present, {counts['missing']} unavailable, "
            f"{counts['failed']} failed — {gb:.2f} GB downloaded."
        )
        QApplication.beep()

    def render_summary(self, summary):
        by_dataset = {}
        for result in summary["results"]:
            bucket = by_dataset.setdefault(result.task.dataset, {k: 0 for k in ("downloaded", "skipped", "missing", "failed")})
            if result.status in bucket:
                bucket[result.status] += 1
        self.table.setRowCount(len(by_dataset))
        for row, (dataset, counts) in enumerate(sorted(by_dataset.items())):
            values = [dataset, counts["downloaded"], counts["skipped"], counts["missing"], counts["failed"]]
            for col, value in enumerate(values):
                self.table.setItem(row, col, QTableWidgetItem(str(value)))
        self.table.resizeColumnsToContents()

    def failed(self, detail):
        self.run_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.status.setText(detail.splitlines()[-1] if detail.splitlines() else detail)


def main():
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    window = MainWindow()
    app._main_window = window
    window.show()
    window.raise_()
    window.activateWindow()
    return app.exec()
