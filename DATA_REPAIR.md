# On-demand Data Repair

Data Repair is intentionally separate from normal archive collection. Normal **Collect / Update Archives** runs do not open CSVs or scan candle continuity, so collection speed is unchanged.

Use Data Repair only when a downstream tool such as Crypto Strategy Lab reports missing candles.

## Workflow

1. Open Binance Data Hub.
2. In **Data Repair — on demand only**, enter the symbol, kline dataset, interval, start date and end date reported by Strategy Lab.
3. Use **Scan Range** to inspect only that local range without downloading anything.
4. Use **Scan & Repair** when you want the Hub to repair detected gaps and verify the range again.

The exact continuity scanner supports fixed UTC kline intervals from `1m` through `1d`. The exchange-anchored `3d`, `1w` and `1mo` intervals are intentionally excluded to avoid false gap reports.

## Repair policy

The repair path preserves Binance's official raw files and chooses the smallest useful repair source:

- a missing or invalid local monthly ZIP is downloaded once as a monthly archive;
- a valid monthly ZIP with internal candle gaps is **not** re-downloaded;
- remaining internal gaps are repaired using only the relevant Binance daily ZIPs;
- monthly and daily timestamps are unioned logically during scans, so a daily repair supplements the monthly archive without modifying it;
- if Binance's daily archive is also unavailable or still lacks the candle, the gap remains visible as unresolved instead of being silently synthesized.

Repair downloads are recorded in the existing `manifest.sqlite`.

## Example

If Strategy Lab reports five missing `1d` candles and 7,200 missing `1m` candles over the same range, the scanner can identify the exact UTC days. Since `5 × 1,440 = 7,200`, this commonly indicates five complete missing UTC days rather than random individual minute gaps.
