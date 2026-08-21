from __future__ import annotations

from PySide6.QtWidgets import QApplication, QFrame, QScrollArea

from .gui import MainWindow


class ResponsiveMainWindow(MainWindow):
    """Main window wrapper that keeps the growing Hub usable on smaller screens."""

    def __init__(self):
        super().__init__()

        content = self.takeCentralWidget()
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidget(content)
        self.setCentralWidget(scroll)

        # Keep data tables compact; each table has its own scrollbar when needed.
        self.benchmark_table.setMaximumHeight(135)
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


def main():
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    window = ResponsiveMainWindow()
    app._main_window = window
    window.show()
    window.raise_()
    window.activateWindow()
    return app.exec()
