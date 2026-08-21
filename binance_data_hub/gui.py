from __future__ import annotations

import traceback
from datetime import datetime

from PySide6.QtCore import QObject, QThread, Signal, Slot, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QFormLayout, QGridLayout, QGroupBox,
    QHBoxLayout, QLabel, QLineEdit, QMainWindow, QMessageBox, QPushButton,
    QProgressBar, QSpinBox, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget,
)

from . import DATA_ROOT
from .downloader import (
    DEFAULT_BENCHMARK_LEVELS,
    DEFAULT_BENCHMARK_SECONDS,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SEGMENTS,
    benchmark_connections,
    download_archive_library,
    recent_benchmark_history,
    recent_run_history,
)
from .catalog import DATASETS, DEFAULT_INTERVALS, INTERVALS, research_core_keys


def _duration(seconds):
    if seconds is None:
        return "—"
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _size(value):
    value = float(value or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TB"


def _speed_text(bps):
    bps = float(bps or 0)
    mb_s = bps / 1024 / 1024
    mbps = bps * 8 / 1_000_000
    return f"{mb_s:.2f} MB/s  ({mbps:.1f} Mbps)"


class Worker(QObject):
    status = Signal(str, int)
    telemetry = Signal(dict)
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
                telemetry=lambda snapshot: self.telemetry.emit(snapshot),
                cancelled=lambda: self.cancelled,
            )
            self.status.emit("Archive collection finished.", 100)
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class BenchmarkWorker(QObject):
    status = Signal(str, int)
    stage = Signal(dict)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, symbol, seconds_per_level):
        super().__init__()
        self.symbol = symbol
        self.seconds_per_level = seconds_per_level
        self.cancelled = False

    @Slot()
    def run(self):
        try:
            def progress(index, total, snapshot):
                elapsed = float(snapshot.get("elapsed_seconds", 0))
                target = max(1.0, float(snapshot.get("target_seconds", self.seconds_per_level)))
                stage_fraction = 1.0 if snapshot.get("stage_complete") else min(1.0, elapsed / target)
                percent = min(99, int(((index - 1) + stage_fraction) / max(1, total) * 100))
                connections = int(snapshot.get("connections", 0))
                speed = float(snapshot.get("average_mbps", snapshot.get("current_mbps", 0)))
                self.status.emit(
                    f"Auto Tune {index}/{total} — {connections} connections: "
                    f"{speed:.1f} Mbps, {_duration(elapsed)} / {_duration(target)}",
                    percent,
                )
                self.stage.emit(snapshot)

            result = benchmark_connections(
                DATA_ROOT,
                self.symbol,
                levels=DEFAULT_BENCHMARK_LEVELS,
                seconds_per_level=self.seconds_per_level,
                progress=progress,
                cancelled=lambda: self.cancelled,
            )
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Binance USD-M Futures Data Hub — Adaptive High-Speed Collector")
        self.resize(1180, 1000)
        self.thread = None
        self.worker = None
        self.operation = None
        self.last_telemetry = {}

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
            "Global cap for active Binance HTTP connections. Use Auto Tune to measure the best value "
            "for the current internet conditions."
        )
        self.benchmark_seconds = QSpinBox()
        self.benchmark_seconds.setRange(5, 30)
        self.benchmark_seconds.setValue(DEFAULT_BENCHMARK_SECONDS)
        self.benchmark_seconds.setSuffix(" sec / level")
        self.benchmark_seconds.setToolTip(
            "Auto Tune tests 4, 8, 16, 24 and 32 connections. 15 seconds per level takes about 75-90 seconds."
        )
        self.verify = QCheckBox("Verify Binance SHA-256 checksums (extra disk read; safer, slightly slower)")
        self.output = QLineEdit(str(DATA_ROOT))
        self.output.setReadOnly(True)
        form.addRow("USD-M symbols", self.symbol)
        form.addRow("Start date", self.start)
        form.addRow("End date", self.end)
        form.addRow("Max HTTP connections", self.connections)
        form.addRow("Auto Tune sample time", self.benchmark_seconds)
        form.addRow("", self.verify)
        form.addRow("Data lake", self.output)
        speed_note = QLabel(
            "Adaptive speed mode: small files use one connection; large monthly archives may use up to four "
            "resumable byte ranges. Auto Tune chooses the smallest connection count that reaches at least 95% "
            "of the fastest measured Binance throughput."
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
        self.benchmark_btn = QPushButton("Speed Benchmark / Auto Tune")
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setEnabled(False)
        open_btn = QPushButton("Open Data Lake")
        self.run_btn.clicked.connect(self.run_download)
        self.benchmark_btn.clicked.connect(self.run_benchmark)
        self.cancel_btn.clicked.connect(self.cancel)
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(str(DATA_ROOT))))
        actions.addWidget(self.run_btn)
        actions.addWidget(self.benchmark_btn)
        actions.addWidget(self.cancel_btn)
        actions.addWidget(open_btn)
        actions.addStretch(1)
        layout.addLayout(actions)

        self.progress = QProgressBar()
        self.status = QLabel("Ready.")
        layout.addWidget(self.progress)
        layout.addWidget(self.status)

        benchmark_box = QGroupBox("Auto Tune results")
        benchmark_layout = QVBoxLayout(benchmark_box)
        self.benchmark_summary = QLabel("Run Auto Tune when internet conditions change. The recommended value is applied automatically.")
        self.benchmark_summary.setWordWrap(True)
        benchmark_layout.addWidget(self.benchmark_summary)
        self.benchmark_table = QTableWidget(0, 5)
        self.benchmark_table.setHorizontalHeaderLabels(["Connections", "Average Mbps", "Average MB/s", "Network", "Errors"])
        self.benchmark_table.setMaximumHeight(155)
        benchmark_layout.addWidget(self.benchmark_table)
        layout.addWidget(benchmark_box)
        self.refresh_benchmark_summary()

        performance = QGroupBox("Live performance")
        perf_grid = QGridLayout(performance)
        self.perf_labels = {}
        fields = (
            ("elapsed", "Elapsed"),
            ("current", "Current speed"),
            ("average", "Average speed"),
            ("peak", "Peak speed"),
            ("network", "Network transferred"),
            ("files_min", "Files / minute"),
            ("eta", "Estimated remaining"),
            ("connections", "Connection cap"),
        )
        for index, (key, title) in enumerate(fields):
            title_label = QLabel(f"{title}:")
            value_label = QLabel("—")
            self.perf_labels[key] = value_label
            row, col = divmod(index, 4)
            perf_grid.addWidget(title_label, row * 2, col)
            perf_grid.addWidget(value_label, row * 2 + 1, col)
        perf_note = QLabel("ETA is based on completed-file rate, so it is approximate when file sizes vary greatly.")
        perf_note.setWordWrap(True)
        perf_grid.addWidget(perf_note, 4, 0, 1, 4)
        layout.addWidget(performance)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Dataset", "Downloaded", "Skipped", "Missing", "Failed"])
        self.table.setMaximumHeight(170)
        layout.addWidget(self.table)

        history_box = QGroupBox("Recent collection performance — compare real runs")
        history_layout = QVBoxLayout(history_box)
        self.history = QTableWidget(0, 8)
        self.history.setHorizontalHeaderLabels([
            "Started", "Connections", "Elapsed", "Avg Mbps", "Peak Mbps", "Network", "Files/min", "Failed"
        ])
        history_layout.addWidget(self.history)
        layout.addWidget(history_box, 1)
        self.refresh_history()

    def select_datasets(self, keys):
        selected = set(keys)
        for key, box in self.dataset_checks.items():
            box.setChecked(key in selected)

    def select_core(self):
        self.select_datasets(research_core_keys())

    def set_busy(self, busy):
        self.run_btn.setEnabled(not busy)
        self.benchmark_btn.setEnabled(not busy)
        self.cancel_btn.setEnabled(busy)

    def reset_performance(self):
        self.last_telemetry = {}
        for label in self.perf_labels.values():
            label.setText("—")
        self.perf_labels["connections"].setText(str(self.connections.value()))

    def run_download(self):
        symbols = [item.strip().upper().replace("/", "") for item in self.symbol.text().replace(",", " ").split() if item.strip()]
        datasets = [key for key, box in self.dataset_checks.items() if box.isChecked()]
        intervals = [key for key, box in self.interval_checks.items() if box.isChecked()]
        needs_intervals = any(DATASETS[key].intervalled for key in datasets)
        if not symbols or not datasets or (needs_intervals and not intervals):
            QMessageBox.warning(self, "Choose data", "Enter one or more symbols, choose datasets, and select at least one interval for kline datasets.")
            return

        self.operation = "download"
        self.set_busy(True)
        self.progress.setValue(0)
        self.status.setText("Planning archive files...")
        self.reset_performance()

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
        self.worker.telemetry.connect(self.set_telemetry)
        self.worker.finished.connect(self.done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    def run_benchmark(self):
        symbols = [item.strip().upper().replace("/", "") for item in self.symbol.text().replace(",", " ").split() if item.strip()]
        symbol = symbols[0] if symbols else "BTCUSDT"
        self.operation = "benchmark"
        self.set_busy(True)
        self.progress.setValue(0)
        total_seconds = self.benchmark_seconds.value() * len(DEFAULT_BENCHMARK_LEVELS)
        self.status.setText(
            f"Finding a recent large Binance archive for Auto Tune. "
            f"Expected test time ~{_duration(total_seconds)}."
        )
        self.benchmark_table.setRowCount(0)

        self.thread = QThread(self)
        self.worker = BenchmarkWorker(symbol, self.benchmark_seconds.value())
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.status.connect(self.set_status)
        self.worker.stage.connect(self.set_benchmark_stage)
        self.worker.finished.connect(self.benchmark_done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    def set_status(self, text, percent):
        self.status.setText(text)
        self.progress.setValue(percent)

    def set_benchmark_stage(self, snapshot):
        if not snapshot.get("stage_complete"):
            return
        rows = self.benchmark_table.rowCount()
        self.benchmark_table.insertRow(rows)
        values = [
            snapshot.get("connections", 0),
            f"{snapshot.get('average_mbps', 0):.2f}",
            f"{snapshot.get('average_mb_s', 0):.2f}",
            _size(snapshot.get("network_bytes", 0)),
            snapshot.get("errors", 0),
        ]
        for col, value in enumerate(values):
            self.benchmark_table.setItem(rows, col, QTableWidgetItem(str(value)))
        self.benchmark_table.resizeColumnsToContents()

    def set_telemetry(self, snapshot):
        self.last_telemetry = snapshot
        self.perf_labels["elapsed"].setText(_duration(snapshot.get("elapsed_seconds")))
        self.perf_labels["current"].setText(_speed_text(snapshot.get("current_bps")))
        self.perf_labels["average"].setText(_speed_text(snapshot.get("average_bps")))
        self.perf_labels["peak"].setText(_speed_text(snapshot.get("peak_bps")))
        self.perf_labels["network"].setText(_size(snapshot.get("network_bytes")))
        self.perf_labels["files_min"].setText(f"{snapshot.get('files_per_minute', 0):.1f}")
        eta = snapshot.get("eta_seconds")
        self.perf_labels["eta"].setText(f"~{_duration(eta)}" if eta is not None else "—")
        self.perf_labels["connections"].setText(str(self.connections.value()))

    def cancel(self):
        if self.worker:
            self.worker.cancelled = True
            if self.operation == "benchmark":
                self.status.setText("Stopping Auto Tune after the current network read...")
            else:
                self.status.setText("Cancelling active downloads; completed ZIPs and resumable parts will be kept.")

    def done(self, summary):
        self.set_busy(False)
        self.operation = None
        self.progress.setValue(100)
        self.render_summary(summary)
        self.set_telemetry(summary.get("performance", {}))
        self.refresh_history()
        counts = summary["counts"]
        perf = summary.get("performance", {})
        segmented = summary.get("segmented_files", 0)
        self.status.setText(
            f"Finished in {_duration(perf.get('elapsed_seconds'))}: {counts['downloaded']} downloaded "
            f"({segmented} segmented), {counts['skipped']} present, {counts['missing']} unavailable, "
            f"{counts['failed']} failed — avg {perf.get('average_mbps', 0):.1f} Mbps, "
            f"peak {perf.get('peak_mbps', 0):.1f} Mbps."
        )
        QApplication.beep()

    def benchmark_done(self, summary):
        self.set_busy(False)
        self.operation = None
        if summary.get("cancelled"):
            self.status.setText("Auto Tune cancelled. No connection setting was changed.")
            return
        self.progress.setValue(100)
        recommended = int(summary["recommended_connections"])
        self.connections.setValue(recommended)
        efficiency = float(summary.get("efficiency_pct", 0))
        self.benchmark_summary.setText(
            f"Recommended: {recommended} connections at {summary['recommended_mbps']:.2f} Mbps. "
            f"Fastest measured: {summary['best_mbps']:.2f} Mbps at {summary['best_connections']} connections. "
            f"The recommendation keeps {efficiency:.1f}% of the best speed while using fewer connections."
        )
        self.status.setText(
            f"Auto Tune complete — use {recommended} connections. "
            f"Measured {summary['recommended_mbps']:.2f} Mbps; best {summary['best_mbps']:.2f} Mbps."
        )
        self.refresh_benchmark_summary(preserve_current=True)
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

    def refresh_benchmark_summary(self, preserve_current=False):
        try:
            rows = recent_benchmark_history(DATA_ROOT, 1)
        except Exception:
            rows = []
        if not rows:
            return
        item = rows[0]
        if not preserve_current:
            self.benchmark_summary.setText(
                f"Last Auto Tune: {item.get('recommended_connections', 0)} connections at "
                f"{item.get('recommended_mbps', 0):.2f} Mbps; fastest test "
                f"{item.get('best_mbps', 0):.2f} Mbps at {item.get('best_connections', 0)} connections."
            )

    def refresh_history(self):
        try:
            rows = recent_run_history(DATA_ROOT, 10)
        except Exception:
            rows = []
        self.history.setRowCount(len(rows))
        for row_index, item in enumerate(rows):
            started = str(item.get("started_at", ""))
            try:
                started = datetime.fromisoformat(started).astimezone().strftime("%Y-%m-%d %H:%M")
            except (TypeError, ValueError):
                started = started[:16].replace("T", " ")
            values = [
                started,
                item.get("max_connections", 0),
                _duration(item.get("elapsed_seconds")),
                f"{item.get('average_mbps', 0):.1f}",
                f"{item.get('peak_mbps', 0):.1f}",
                _size(item.get("network_bytes", 0)),
                f"{item.get('files_per_minute', 0):.1f}",
                item.get("failed_files", 0),
            ]
            for col, value in enumerate(values):
                self.history.setItem(row_index, col, QTableWidgetItem(str(value)))
        self.history.resizeColumnsToContents()

    def failed(self, detail):
        self.set_busy(False)
        self.operation = None
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
