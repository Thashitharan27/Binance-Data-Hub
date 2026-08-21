from __future__ import annotations
import traceback
from PySide6.QtCore import QObject,QThread,Signal,Slot,QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QApplication,QCheckBox,QComboBox,QFormLayout,QGroupBox,QHBoxLayout,QLabel,QLineEdit,QMainWindow,QMessageBox,QPushButton,QProgressBar,QTableWidget,QTableWidgetItem,QVBoxLayout,QWidget
from . import DATA_ROOT
from .downloader import download_klines

class Worker(QObject):
    status=Signal(str,int);finished=Signal(list);failed=Signal(str)
    def __init__(self,symbol,intervals,start,end):super().__init__();self.symbol=symbol;self.intervals=intervals;self.start=start;self.end=end;self.cancelled=False
    @Slot()
    def run(self):
        results=[]
        try:
            for number,interval in enumerate(self.intervals,1):
                base=int((number-1)/len(self.intervals)*100);self.status.emit(f"{self.symbol} {interval}: connecting...",base)
                item=download_klines(self.symbol,interval,DATA_ROOT/f"{self.symbol}_{interval}.csv",self.start,self.end,progress_state=lambda n,total,d,i=interval,index=number:self.status.emit(f"{self.symbol} {i}: {n:,} of about {total:,} candles prepared; {d}",min(99,int(((index-1)+(n/max(1,total)))/len(self.intervals)*100))),cancelled=lambda:self.cancelled)
                results.append({"symbol":self.symbol,"interval":interval,**item})
            archives=sum(item.get("archives",0) for item in results);gaps=sum(item.get("gaps",0) for item in results)
            self.status.emit(f"All selected datasets are current. {archives} archives verified; {gaps} timestamp gaps detected.",100);self.finished.emit(results)
        except Exception:self.failed.emit(traceback.format_exc())

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__();self.setWindowTitle("Binance USD-M Futures Data Hub");self.resize(880,620);self.thread=None;self.worker=None
        root=QWidget();self.setCentralWidget(root);layout=QVBoxLayout(root)
        intro=QLabel("One shared Binance USD-M perpetual Futures candle library for every bot and backtester. Downloads run in the background, so keep this window open or minimized while you work.");intro.setWordWrap(True);layout.addWidget(intro)
        group=QGroupBox("Download / Update");form=QFormLayout(group);self.symbol=QComboBox();self.symbol.setEditable(True);self.symbol.addItems(["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","DOGEUSDT"])
        intervals=QWidget();row=QHBoxLayout(intervals);row.setContentsMargins(0,0,0,0);self.checks={}
        for interval in ("1m","5m","15m","30m","1h","4h","1d"):
            box=QCheckBox(interval);box.setChecked(interval in ("1m","1h","4h"));self.checks[interval]=box;row.addWidget(box)
        self.start=QLineEdit("2017-01-01");self.end=QLineEdit();self.end.setPlaceholderText("Today / latest closed candle")
        folder=QLineEdit(str(DATA_ROOT));folder.setReadOnly(True);form.addRow("Pair",self.symbol);form.addRow("Timeframes",intervals);form.addRow("Start date",self.start);form.addRow("End date",self.end);form.addRow("Shared folder",folder);layout.addWidget(group)
        actions=QHBoxLayout();self.run_btn=QPushButton("Download / Update in Background");self.pause_btn=QPushButton("Pause Safely");self.pause_btn.setEnabled(False);open_btn=QPushButton("Open Shared Folder")
        self.run_btn.clicked.connect(self.run_download);self.pause_btn.clicked.connect(self.pause);open_btn.clicked.connect(lambda:QDesktopServices.openUrl(QUrl.fromLocalFile(str(DATA_ROOT))))
        actions.addWidget(self.run_btn);actions.addWidget(self.pause_btn);actions.addWidget(open_btn);layout.addLayout(actions)
        self.progress=QProgressBar();self.status=QLabel("Ready.");layout.addWidget(self.progress);layout.addWidget(self.status)
        self.table=QTableWidget(0,4);self.table.setHorizontalHeaderLabels(["Pair","Timeframe","File size","Path"]);layout.addWidget(self.table,1);self.refresh()
    def refresh(self):
        DATA_ROOT.mkdir(parents=True,exist_ok=True);files=sorted(DATA_ROOT.glob("*.csv"));self.table.setRowCount(len(files))
        for r,path in enumerate(files):
            symbol,interval=path.stem.rsplit("_",1)
            for c,value in enumerate((symbol,interval,f"{path.stat().st_size/1024/1024:.1f} MB",str(path))):self.table.setItem(r,c,QTableWidgetItem(value))
        self.table.resizeColumnsToContents()
    def run_download(self):
        symbol=self.symbol.currentText().strip().upper().replace("/","");intervals=[k for k,v in self.checks.items() if v.isChecked()]
        if not symbol or not intervals:QMessageBox.warning(self,"Choose data","Enter a pair and select at least one timeframe.");return
        self.run_btn.setEnabled(False);self.pause_btn.setEnabled(True);self.thread=QThread(self);self.worker=Worker(symbol,intervals,self.start.text().strip() or None,self.end.text().strip() or None);self.worker.moveToThread(self.thread);self.thread.started.connect(self.worker.run);self.worker.status.connect(self.set_status);self.worker.finished.connect(self.done);self.worker.failed.connect(self.failed);self.worker.finished.connect(self.thread.quit);self.worker.failed.connect(self.thread.quit);self.thread.start()
    def set_status(self,text,percent):self.status.setText(text);self.progress.setValue(percent)
    def pause(self):
        if self.worker:self.worker.cancelled=True;self.status.setText("Pausing safely after the current Binance request...")
    def done(self,_):self.run_btn.setEnabled(True);self.pause_btn.setEnabled(False);self.refresh();QApplication.beep()
    def failed(self,detail):self.run_btn.setEnabled(True);self.pause_btn.setEnabled(False);self.status.setText(detail.splitlines()[-1] if detail.splitlines() else detail);self.refresh()

def main():
    app=QApplication.instance() or QApplication([]);app.setStyle("Fusion");window=MainWindow();app._main_window=window;window.show();window.raise_();window.activateWindow();return app.exec()
