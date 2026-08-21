# Binance Data Hub

Shared Binance USD-M perpetual Futures OHLCV downloader for every bot and backtester under `C:\CryptoBots`.

Futures data lives in `C:\CryptoBots\Binance Market Data\futures\usdm` with predictable names such as `BTCUSDT_1m.csv`, `BTCUSDT_1h.csv`, and `BTCUSDT_4h.csv`. Files use `timestamp,open,high,low,close,volume`, UTC Binance candle-open timestamps, atomic publishing, and resumable checkpoints.

Old Spot files remain isolated under `C:\CryptoBots\Binance Market Data\spot` and are never selected by Futures backtests.

Double-click `Open Binance Data Hub.bat`, choose a pair and timeframes, and press **Download / Update in Background**. The window may remain open or minimized while you work. Bots should read these files but should not modify them.

Large historical downloads are automatic and archive-first. The hub downloads up to four official Binance monthly USD-M Futures archives concurrently, verifies their SHA-256 checksums, extracts them into resumable parts, and uses the Binance REST API for unavailable archives and the latest partial month. It then sorts, deduplicates, validates timestamps, and atomically publishes the final CSV. No manual archive downloading or combining is required, and the existing CSV remains usable until the update is complete.
