"""Runtime GUI entry point with verified aggTrades repair enabled."""
from __future__ import annotations

from . import runtime_gui as _runtime_gui
from .verified_repair import scan_and_repair_kline_range, scan_kline_range


_ORIGINAL_REPAIR_DONE = _runtime_gui.RuntimeMainWindow.repair_done


def _enhanced_repair_progress(self, stage, done, total, detail):
    ranges = {
        "scan-before": (0, 15, "Initial scan"),
        "repair-monthly": (15, 30, "Repairing monthly archive"),
        "scan-middle": (30, 40, "Checking monthly repair"),
        "repair-daily": (40, 55, "Repairing affected UTC day"),
        "scan-after-daily": (55, 65, "Checking daily kline repair"),
        "repair-aggtrades": (65, 80, "Downloading daily aggTrades evidence"),
        "reconstruct-aggtrades": (80, 90, "Verifying candle from aggTrades"),
        "scan-after": (90, 99, "Verifying repaired range"),
    }
    low, high, label = ranges.get(stage, (0, 99, stage))
    fraction = done / max(1, total)
    percent = min(99, int(low + (high - low) * fraction))
    if hasattr(detail, "task"):
        task = detail.task
        extra = f" — {task.key}: {detail.status}"
    else:
        key = detail.get("key", "") if isinstance(detail, dict) else ""
        status = detail.get("status", "") if isinstance(detail, dict) else ""
        extra = f" — {key}" if key else ""
        if status:
            extra += f": {status}"
    self.status.emit(f"{label} {done:,}/{total:,}{extra}", percent)


def _enhanced_repair_done(self, summary):
    _ORIGINAL_REPAIR_DONE(self, summary)
    if summary.get("mode") != "repair":
        return

    verified_days = set(summary.get("verified_repair_days", []))
    if not verified_days:
        return

    for row in range(self.repair_table.rowCount()):
        day_item = self.repair_table.item(row, 0)
        if day_item is None or day_item.text() not in verified_days:
            continue
        self.repair_table.setItem(
            row,
            2,
            _runtime_gui.QTableWidgetItem("daily aggTrades verified reconstruction"),
        )
        status_item = self.repair_table.item(row, 3)
        status = status_item.text() if status_item is not None else "Repaired"
        if "aggTrades" not in status:
            status = "Repaired · verified from Binance aggTrades"
        self.repair_table.setItem(row, 3, _runtime_gui.QTableWidgetItem(status))

    reconstructed = int(summary.get("reconstructed_candles", 0))
    downloads = int(summary.get("aggtrade_repairs", 0))
    self.repair_summary.setText(
        self.repair_summary.text()
        + f" Verified aggTrades fallback: {downloads:,} daily archive attempt(s), "
        + f"{reconstructed:,} candle(s) reconstructed with provenance."
    )
    self.repair_table.resizeColumnsToContents()


def main():
    # RepairWorker resolves these names from runtime_gui at execution time, so
    # wiring the enhanced implementations here keeps the existing GUI intact
    # while giving Data Repair the verified aggTrades fallback.
    _runtime_gui.scan_kline_range = scan_kline_range
    _runtime_gui.scan_and_repair_kline_range = scan_and_repair_kline_range
    _runtime_gui.RepairWorker._repair_progress = _enhanced_repair_progress
    _runtime_gui.RuntimeMainWindow.repair_done = _enhanced_repair_done
    return _runtime_gui.main()
