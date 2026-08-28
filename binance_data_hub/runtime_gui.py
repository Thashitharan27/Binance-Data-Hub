from __future__ import annotations

import traceback

from PySide6.QtCore import QObject, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFormLayout, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QMessageBox, QPushButton, QSpinBox, QTableWidget,
    QTableWidgetItem, QVBoxLayout,
)

from . import DATA_ROOT
from .catalog import DATASETS
from .downloader import DEFAULT_SEGMENTS
from .responsive_gui import ResponsiveMainWindow
from .runtime_download import download_archive_library_runtime
from .repair import (
    INTERVAL_MILLISECONDS,
    KLINE_DATASETS,
    scan_and_repair_kline_range,
    scan_kline_range,
)


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


class RepairWorker(QObject):
    status = Signal(str, int)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, symbol, dataset, interval, start, end, repair, connections, verify):
        super().__init__()
        self.symbol = symbol
        self.dataset = dataset
        self.interval = interval
        self.start = start
        self.end = end
        self.repair = repair
        self.connections = connections
        self.verify = verify
        self.cancelled = False

    def _repair_progress(self, stage, done, total, detail):
        ranges = {
            "scan-before": (0, 20, "Initial scan"),
            "repair-monthly": (20, 45, "Repairing monthly archive"),
            "scan-middle": (45, 60, "Checking monthly repair"),
            "repair-daily": (60, 85, "Repairing affected UTC day"),
            "scan-after": (85, 99, "Verifying repaired range"),
        }
        low, high, label = ranges.get(stage, (0, 99, stage))
        fraction = done / max(1, total)
        percent = min(99, int(low + (high - low) * fraction))
        if hasattr(detail, "task"):
            task = detail.task
            extra = f" — {task.key}: {detail.status}"
        else:
            key = detail.get("key", "") if isinstance(detail, dict) else ""
            extra = f" — {key}" if key else ""
        self.status.emit(f"{label} {done:,}/{total:,}{extra}", percent)

    @Slot()
    def run(self):
        try:
            if self.repair:
                result = scan_and_repair_kline_range(
                    DATA_ROOT,
                    self.symbol,
                    self.dataset,
                    self.interval,
                    self.start,
                    self.end,
                    verify=self.verify,
                    max_connections=self.connections,
                    progress=self._repair_progress,
                    cancelled=lambda: self.cancelled,
                )
                result["mode"] = "repair"
            else:
                def progress(done, total, info):
                    percent = min(99, int(done / max(1, total) * 100))
                    self.status.emit(
                        f"Scanning local archive {done:,}/{total:,} — {info.get('key', '')}",
                        percent,
                    )

                scan = scan_kline_range(
                    DATA_ROOT,
                    self.symbol,
                    self.dataset,
                    self.interval,
                    self.start,
                    self.end,
                    progress=progress,
                    cancelled=lambda: self.cancelled,
                )
                result = {"mode": "scan", "scan": scan, "cancelled": scan.get("cancelled", False)}
            self.finished.emit(result)
        except Exception:
            self.failed.emit(traceback.format_exc())


class RuntimeMainWindow(ResponsiveMainWindow):
    """Responsive Hub UI with optional in-run tuning and on-demand data repair."""

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

        repair_box = QGroupBox("Data Repair — on demand only")
        repair_layout = QVBoxLayout(repair_box)
        repair_note = QLabel(
            "Use this only when Strategy Lab reports missing or invalid candles. It scans the selected local "
            "range only; normal Collect / Update Archives runs never open CSVs or perform continuity/integrity "
            "scans. Scan & Repair keeps valid monthly ZIPs intact and uses targeted daily Binance archives to "
            "fill missing candles or replace invalid monthly rows."
        )
        repair_note.setWordWrap(True)
        repair_layout.addWidget(repair_note)

        repair_form = QFormLayout()
        self.repair_symbol = QLineEdit("BTCUSDT")
        self.repair_symbol.setPlaceholderText("BTCUSDT")
        self.repair_dataset = QComboBox()
        for key in KLINE_DATASETS:
            self.repair_dataset.addItem(DATASETS[key].label, key)
        self.repair_interval = QComboBox()
        for interval in INTERVAL_MILLISECONDS:
            self.repair_interval.addItem(interval, interval)
        self.repair_start = QLineEdit()
        self.repair_start.setPlaceholderText("YYYY-MM-DD")
        self.repair_end = QLineEdit()
        self.repair_end.setPlaceholderText("YYYY-MM-DD")
        repair_form.addRow("Symbol", self.repair_symbol)
        repair_form.addRow("Kline dataset", self.repair_dataset)
        repair_form.addRow("Interval", self.repair_interval)
        repair_form.addRow("Start date", self.repair_start)
        repair_form.addRow("End date", self.repair_end)
        repair_layout.addLayout(repair_form)

        repair_actions = QHBoxLayout()
        self.repair_scan_btn = QPushButton("Scan Range")
        self.repair_fix_btn = QPushButton("Scan & Repair")
        self.repair_scan_btn.setToolTip(
            "Read only the selected local ZIPs and report missing candles plus blocking kline integrity errors. Downloads nothing."
        )
        self.repair_fix_btn.setToolTip(
            "Scan first, then download only the smallest useful monthly/daily repair archives and scan again."
        )
        self.repair_scan_btn.clicked.connect(lambda: self.start_repair_operation(False))
        self.repair_fix_btn.clicked.connect(lambda: self.start_repair_operation(True))
        repair_actions.addWidget(self.repair_scan_btn)
        repair_actions.addWidget(self.repair_fix_btn)
        repair_actions.addStretch(1)
        repair_layout.addLayout(repair_actions)

        self.repair_summary = QLabel("No repair scan run yet.")
        self.repair_summary.setWordWrap(True)
        repair_layout.addWidget(self.repair_summary)
        self.repair_table = QTableWidget(0, 4)
        self.repair_table.setHorizontalHeaderLabels(["UTC day", "Missing / invalid", "Repair source", "Status"])
        self.repair_table.setMaximumHeight(180)
        repair_layout.addWidget(self.repair_table)

        # Keep repair next to the collection controls but fully separate from the
        # collection execution path.
        root_layout.insertWidget(3, repair_box)

    def set_busy(self, busy):
        super().set_busy(busy)
        if hasattr(self, "auto_recalibrate"):
            self.auto_recalibrate.setEnabled(not busy)
            self.recalibration_minutes.setEnabled(not busy)
        if hasattr(self, "repair_scan_btn"):
            for widget in (
                self.repair_symbol,
                self.repair_dataset,
                self.repair_interval,
                self.repair_start,
                self.repair_end,
                self.repair_scan_btn,
                self.repair_fix_btn,
            ):
                widget.setEnabled(not busy)

    def run_download(self):
        selection = self._selected_request()
        if selection is None:
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

    def _repair_request(self):
        symbol = self.repair_symbol.text().strip().upper().replace("/", "")
        dataset = self.repair_dataset.currentData()
        interval = self.repair_interval.currentData()
        start = self.repair_start.text().strip()
        end = self.repair_end.text().strip()
        if not symbol or not start or not end:
            QMessageBox.warning(
                self,
                "Repair range required",
                "Enter a symbol plus the exact start and end dates reported by Strategy Lab.",
            )
            return None
        return symbol, dataset, interval, start, end

    def start_repair_operation(self, repair):
        request = self._repair_request()
        if request is None:
            return
        symbol, dataset, interval, start, end = request

        self.operation = "repair-fix" if repair else "repair-scan"
        self.set_busy(True)
        self.progress.setValue(0)
        self.repair_table.setRowCount(0)
        self.repair_summary.setText(
            "Scanning and repairing only this range..."
            if repair
            else "Scanning only this local range for missing/invalid candles; nothing will be downloaded..."
        )
        self.status.setText(self.repair_summary.text())

        self.thread = QThread(self)
        self.worker = RepairWorker(
            symbol,
            dataset,
            interval,
            start,
            end,
            repair,
            self.connections.value(),
            self.verify.isChecked(),
        )
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.status.connect(self.set_status)
        self.worker.finished.connect(self.repair_done)
        self.worker.failed.connect(self.failed)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.start()

    @staticmethod
    def _integrity_codes_by_day(scan):
        result = {}
        for detail in scan.get("integrity_issues", []):
            day = detail.get("day")
            if not day:
                continue
            result.setdefault(day, set()).update(str(code) for code in detail.get("codes", []))
        return result

    def repair_done(self, summary):
        self.set_busy(False)
        self.operation = None
        if summary.get("cancelled"):
            self.status.setText("Data Repair operation cancelled.")
            self.repair_summary.setText("Cancelled before the selected range was fully checked.")
            return

        mode = summary.get("mode")
        if mode == "scan":
            before = summary.get("scan", {})
            after = before
        else:
            before = summary.get("before", {})
            after = summary.get("after", {})

        before_missing = before.get("missing_by_day", {})
        before_invalid = before.get("invalid_by_day", {})
        after_missing = after.get("missing_by_day", {})
        after_invalid = after.get("invalid_by_day", {})
        codes_by_day = self._integrity_codes_by_day(after)

        if mode == "repair":
            days = sorted(
                set(before_missing)
                | set(before_invalid)
                | set(after_missing)
                | set(after_invalid)
            )
            source_missing = set(summary.get("source_missing_days", []))
            failed = set(summary.get("failed_days", []))
            rows = []
            for day in days:
                remaining_missing = int(after_missing.get(day, 0))
                remaining_invalid = int(after_invalid.get(day, 0))
                remaining = f"{remaining_missing:,} / {remaining_invalid:,}"
                codes = ", ".join(sorted(codes_by_day.get(day, ())))
                if remaining_missing == 0 and remaining_invalid == 0:
                    status = "Repaired"
                elif day in source_missing:
                    status = "Binance daily archive unavailable"
                elif day in failed:
                    status = "Repair download failed"
                elif remaining_missing and remaining_invalid:
                    status = "Still missing + invalid after repair"
                elif remaining_missing:
                    status = "Still missing after repair"
                else:
                    status = "Still invalid after repair"
                if codes:
                    status += f" · {codes}"
                rows.append((day, remaining, "monthly/daily targeted repair", status))
        else:
            days = sorted(set(after_missing) | set(after_invalid))
            rows = []
            for day in days:
                missing = int(after_missing.get(day, 0))
                invalid = int(after_invalid.get(day, 0))
                codes = ", ".join(sorted(codes_by_day.get(day, ())))
                if missing and invalid:
                    status = "Missing + invalid"
                elif missing:
                    status = "Missing"
                else:
                    status = "Invalid"
                if codes:
                    status += f" · {codes}"
                rows.append((day, f"{missing:,} / {invalid:,}", "not attempted", status))

        self.repair_table.setRowCount(len(rows))
        for row_index, values in enumerate(rows):
            for col, value in enumerate(values):
                self.repair_table.setItem(row_index, col, QTableWidgetItem(str(value)))
        self.repair_table.resizeColumnsToContents()

        invalid_archives = len(after.get("invalid_archives", []))
        duplicates = int(after.get("archive_duplicates", 0))
        issue_counts = after.get("integrity_issue_counts", {})
        issue_summary = ", ".join(
            f"{code}: {int(count):,}" for code, count in sorted(issue_counts.items())
        ) or "none"

        if mode == "repair":
            self.repair_summary.setText(
                f"Before: {before.get('missing_candles', 0):,} missing candles across "
                f"{before.get('missing_days', 0):,} UTC days; "
                f"{before.get('invalid_candles', 0):,} invalid candles across "
                f"{before.get('invalid_days', 0):,} days. After repair: "
                f"{after.get('missing_candles', 0):,} missing across {after.get('missing_days', 0):,} days; "
                f"{after.get('invalid_candles', 0):,} invalid across {after.get('invalid_days', 0):,} days. "
                f"Monthly repair attempts: {summary.get('monthly_repairs', 0):,}; "
                f"daily repair attempts: {summary.get('daily_repairs', 0):,}. "
                f"Integrity issues remaining: {issue_summary}. "
                f"Invalid local archives remaining: {invalid_archives:,}; "
                f"duplicate rows inside individual archives: {duplicates:,}."
            )
        else:
            self.repair_summary.setText(
                f"Expected {after.get('expected_candles', 0):,} candles; found {after.get('found_candles', 0):,}; "
                f"missing {after.get('missing_candles', 0):,} across {after.get('missing_days', 0):,} UTC days; "
                f"invalid {after.get('invalid_candles', 0):,} across {after.get('invalid_days', 0):,} days. "
                f"Integrity issues: {issue_summary}. "
                f"Scanned {after.get('archives_scanned', 0):,} local archives. "
                f"Invalid archives: {invalid_archives:,}; duplicate rows inside individual archives: {duplicates:,}."
            )

        if after.get("complete"):
            self.status.setText("Data Repair check complete — selected range is healthy.")
        elif mode == "repair":
            self.status.setText(
                f"Repair finished — {after.get('missing_candles', 0):,} candles remain missing and "
                f"{after.get('invalid_candles', 0):,} remain invalid. "
                "Review the UTC-day table for Binance source gaps, integrity errors, or failed downloads."
            )
        else:
            self.status.setText(
                f"Scan complete — {after.get('missing_candles', 0):,} missing and "
                f"{after.get('invalid_candles', 0):,} invalid candles found."
            )
        self.progress.setValue(100)
        QApplication.beep()

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
