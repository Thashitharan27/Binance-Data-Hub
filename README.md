# Binance Data Hub

High-speed collector for Binance **USD-M Futures** public historical archives.

The project is intentionally a **data collection layer**, not a strategy engine. It mirrors Binance's official compressed archive files into one shared data lake under:

```text
C:\CryptoBots\Binance Market Data
```

## Design: speed first

The Hub keeps Binance's official `.zip` archives directly instead of extracting and merging giant CSV files during collection.

Current transport behavior:

- small/daily archives use one resumable HTTP stream each;
- large monthly 1m/3m/5m kline-family, trade and order-book ZIPs are probed for HTTP Range support;
- eligible large ZIPs automatically use up to four parallel byte-range streams;
- byte ranges are reassembled into the exact original Binance ZIP;
- one global HTTP connection cap prevents segmented files from multiplying into uncontrolled connection counts;
- partially downloaded normal files and byte-range segments are resumable;
- existing archives are skipped;
- optional Binance `.CHECKSUM` SHA-256 verification is available;
- completed downloads are validated as ZIP archives before publication;
- monthly archives are preferred for completed months and daily archives cover the current partial month;
- missing monthly archives automatically fall back to daily files when that archive family supports both.

The objective is maximum useful throughput with bounded network pressure and minimal CPU/disk work.

## Speed Benchmark / Auto Tune

Internet capacity and international routing can change significantly by time of day, so the Hub can benchmark Binance directly instead of relying on a fixed connection setting.

Press **Speed Benchmark / Auto Tune**. By default the Hub tests:

```text
4 -> 8 -> 16 -> 24 -> 32 connections
```

Each level downloads temporary byte ranges from a recent large Binance public archive for 15 seconds by default. Benchmark bytes are discarded rather than added to the data lake. The sample time is configurable from 5 to 30 seconds per level.

The benchmark reports average Mbps, MB/s, transferred bytes and request errors for each connection count. It then automatically selects the **smallest connection count that reaches at least 95% of the fastest measured throughput**. This avoids using 24 or 32 connections when, for example, 8 connections already saturate a slower internet line.

The recommended value is applied to **Max HTTP connections** automatically. Benchmark results are stored in the `speed_benchmarks` table inside `manifest.sqlite`, so the most recent recommendation remains visible after the app restarts.

Running Auto Tune again during peak/off-peak hours is useful when available bandwidth changes materially.

## Live performance telemetry

Every collection run measures itself so connection-count tuning can be based on the actual PC, internet connection and Binance CDN path rather than guesses.

The GUI shows live:

- elapsed time;
- current **MB/s and Mbps**;
- average speed;
- peak speed;
- actual network bytes transferred during the run;
- files per minute;
- approximate remaining time based on completed-file rate;
- configured global connection cap.

A performance record is written after every completed collection run to the `download_runs` table inside `manifest.sqlite`. The GUI shows the most recent runs side-by-side, including connection count, elapsed time, average/peak Mbps, network bytes, files/minute and failures.

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
5. the global **Max HTTP connections** value, or run **Speed Benchmark / Auto Tune** first.

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
