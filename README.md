# Binance Data Hub

High-speed collector for Binance **USD-M Futures** public historical archives.

The project is intentionally a **data collection layer**, not a strategy engine. It mirrors Binance's official compressed archive files into one shared data lake under:

```text
C:\CryptoBots\Binance Market Data
```

## Design: speed first

Version 3 no longer downloads candles and immediately extracts/merges them into giant CSV files.

Instead it:

- downloads Binance's official `.zip` archives directly;
- uses up to 64 parallel workers (16 by default) across the planned archive queue;
- streams large files to disk instead of loading ZIPs into RAM;
- resumes a partially downloaded archive with HTTP Range requests when the server supports it;
- skips files that already exist;
- optionally verifies Binance `.CHECKSUM` SHA-256 files;
- records download status in `manifest.sqlite`;
- uses one monthly archive for completed months whenever possible;
- uses daily archives for the current partial month;
- automatically falls back from a missing monthly archive to daily files when the dataset supports daily archives.

This minimizes network requests, decompression time, CSV parsing, and disk writes. Downstream tools can later read or materialize only the data they need.

## Collected USD-M Futures datasets

The GUI can collect:

- `klines` — full Binance kline files, including quote volume, trade count, taker-buy base volume and taker-buy quote volume;
- `metrics` — historical open interest plus top-trader/global long-short and taker long-short volume ratios;
- `fundingRate`;
- `markPriceKlines`;
- `indexPriceKlines`;
- `premiumIndexKlines`;
- `aggTrades`;
- `trades`;
- `bookDepth`;
- `bookTicker`.

`bookTicker` archive coverage is known to be discontinuous/stale for some symbols and dates, so missing files are recorded rather than treated as fatal errors. Historical liquidation snapshots are not included because Binance removed that public archive family.

## Storage layout

Files preserve the Binance archive hierarchy:

```text
C:\CryptoBots\Binance Market Data\
  manifest.sqlite
  raw\
    futures\
      um\
        monthly\
          klines\BTCUSDT\1h\BTCUSDT-1h-2025-01.zip
          fundingRate\BTCUSDT\BTCUSDT-fundingRate-2025-01.zip
          markPriceKlines\BTCUSDT\1h\...
        daily\
          metrics\BTCUSDT\BTCUSDT-metrics-2026-08-20.zip
          bookDepth\BTCUSDT\BTCUSDT-bookDepth-2026-08-20.zip
          klines\BTCUSDT\1h\...
```

The downloaded ZIP files are market data and should **not** be committed to GitHub.

## GUI

Double-click:

```text
Open Binance Data Hub.bat
```

Choose:

1. one or more symbols (comma/space separated);
2. historical date range;
3. datasets;
4. intervals for kline-type datasets;
5. number of parallel downloads.

Use **Research Core** for the derivatives-context datasets most useful for strategy research, or **Select Everything** to mirror every supported archive family.

Checksum verification is optional. Leave it off for maximum collection speed; turn it on when you want end-to-end archive integrity verification.

## Notes on Binance archives

Binance publishes daily files after the day completes and monthly files after a month completes. The collector excludes the current UTC day because a complete daily archive is normally not available yet.

Some official Binance archive families contain occasional historical gaps or duplicates. The Hub preserves the official source files unchanged and records unavailable files in the manifest rather than silently synthesizing missing market data.

## Development

Run tests from the repository root:

```powershell
python -m pytest tests -q
```
