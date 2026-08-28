# Verified kline repair from daily aggTrades

Data Repair keeps Binance's official monthly and daily kline ZIPs immutable. When an ordinary daily-kline repair still contains a narrow volume-family integrity error, the GUI now has a final evidence-based fallback for **Contract klines**.

## Automatic fallback

For unresolved `TAKER_VOLUME_EXCEEDS_TOTAL` or `TAKER_QUOTE_VOLUME_EXCEEDS_TOTAL` candles, **Scan & Repair**:

1. Requests only the affected UTC day's Binance `daily/aggTrades` ZIP. It does not use the normal monthly-preference planner.
2. Reconstructs the affected candle from the aggregate trades.
3. Requires the aggregate trade-id ranges to be contiguous and the reconstructed individual trade count to equal Binance's original kline `trade_count`.
4. Requires reconstructed OHLC to agree with the original kline, because this fallback is for volume metadata anomalies rather than rewriting historical prices.
5. Requires at least three of the four original volume-family fields (`volume`, `quote_volume`, taker-buy base volume, taker-buy quote volume) to agree with the reconstruction. This makes the fallback a conservative single-field correction, not a general data synthesizer.
6. Re-runs the normal kline integrity checks on the reconstructed row.

If any proof step fails, the candle remains invalid and the repair is reported as unresolved.

## Repair overlay

A successful reconstruction is stored separately under:

`raw/futures/um/daily/klines/<SYMBOL>/<INTERVAL>/zz_verified_repairs/`

The overlay ZIP contains only reconstructed timestamps plus `repair_manifest.json`, which records the original issue codes, original row, reconstructed row, source kline SHA-256 and source aggTrades SHA-256.

The original Binance monthly kline, daily kline and aggTrades archives are never edited.

Crypto Strategy Lab's existing overlapping-source repair policy can consume the valid later overlay while retaining the invalid source rows for provenance.

## Why aggTrades is not in the Data Repair dropdown

The selected dataset remains **Contract klines** because that is the dataset being repaired. `aggTrades` is an automatic evidence source used only when the daily kline cannot repair a qualifying invalid candle.
