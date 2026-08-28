"""Runtime GUI entry point with verified aggTrades repair enabled."""
from __future__ import annotations

from . import runtime_gui as _runtime_gui
from .verified_repair import scan_and_repair_kline_range, scan_kline_range


def main():
    # RepairWorker resolves these names from runtime_gui at execution time, so
    # wiring the enhanced implementations here keeps the existing GUI intact
    # while giving Data Repair the verified aggTrades fallback.
    _runtime_gui.scan_kline_range = scan_kline_range
    _runtime_gui.scan_and_repair_kline_range = scan_and_repair_kline_range
    return _runtime_gui.main()
