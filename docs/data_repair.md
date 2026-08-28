# On-demand Data Repair

Data Repair is intentionally separate from normal archive collection. Normal **Collect / Update Archives** runs do not open CSVs or scan candle continuity/integrity, so collection speed is unchanged.

Use Data Repair only when a downstream tool such as Crypto Strategy Lab reports missing or invalid candles.

## Workflow

1. Open Binance Data Hub.
2. In **Data Repair — on demand only**, enter the symbol, kline dataset, interval, start date and end date reported by Strategy Lab.
3. Use **Scan Range** to inspect only that local range without downloading anything.
4. Use **Scan & Repair** when you want the Hub to repair detected gaps or invalid candles and verify the range again.

The exact continuity scanner supports fixed UTC kline intervals from `1m` through `1d`. The exchange-anchored `3d`, `1w` and `1mo` intervals are intentionally excluded to avoid false gap reports.

## What the scanner checks

The repair scanner checks both **continuity** and basic **kline integrity**:

- every expected fixed-interval candle timestamp is present;
- timestamps sit on the expected UTC interval grid;
- required OHLCV fields are finite and in valid domains;
- OHLC values obey ordinary candle bounds;
- optional quote/taker fields are non-negative when present;
- `taker_buy_base_volume <= volume`;
- `taker_buy_quote_volume <= quote_volume` when both are present.

This catches cases where a candle exists on the timeline but contains an internally impossible value, such as `TAKER_VOLUME_EXCEEDS_TOTAL`.

## Repair policy

The repair path preserves Binance's official raw files and chooses the smallest useful repair source:

- a missing or unreadable/corrupt local monthly ZIP is downloaded once as a monthly archive;
- a valid monthly ZIP with internal candle gaps is **not** re-downloaded;
- a valid monthly ZIP containing an invalid candle value is also **not** edited or re-downloaded just to rewrite that row;
- remaining missing **or invalid** UTC days are repaired using only the relevant Binance daily kline ZIPs;
- monthly and daily rows are combined logically during scans, with daily rows taking precedence for matching timestamps, so a daily repair can replace one bad monthly row without modifying the monthly archive;
- for unresolved Contract-kline volume-family anomalies that remain invalid in the daily kline, Data Repair can automatically fetch only the affected UTC day's `daily/aggTrades` archive and attempt a verified reconstruction;
- if the aggTrades proof requirements do not reconcile, the candle remains invalid rather than being silently synthesized.

Repair downloads are recorded in the existing `manifest.sqlite`.

See [Verified kline repair](verified_kline_repair.md) for the aggTrades reconstruction proof and provenance rules.

## Example

If Strategy Lab reports five missing `1d` candles and 7,200 missing `1m` candles over the same range, the scanner can identify the exact UTC days. Since `5 × 1,440 = 7,200`, this commonly indicates five complete missing UTC days rather than random individual minute gaps.

If Strategy Lab instead reports a present candle such as `TAKER_VOLUME_EXCEEDS_TOTAL` at `2023-11-30 12:35 UTC`, Data Repair first requests the Binance daily kline for that day. If that daily row is also invalid, the verified repair fallback can use that day's aggTrades evidence without modifying the original Binance archives.
