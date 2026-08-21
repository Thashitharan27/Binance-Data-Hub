from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    label: str
    monthly: bool
    daily: bool
    intervalled: bool = False
    research_core: bool = False
    note: str = ""


DATASETS: dict[str, DatasetSpec] = {
    "klines": DatasetSpec(
        "klines", "Contract klines (full Binance fields)", True, True, True, True,
        "Price/volume candles including quote volume, trade count and taker-buy volume.",
    ),
    "metrics": DatasetSpec(
        "metrics", "Futures metrics (OI + long/short + taker ratio)", False, True, False, True,
        "5-minute historical futures metrics where Binance publishes them.",
    ),
    "fundingRate": DatasetSpec(
        "fundingRate", "Funding rate", True, False, False, True,
        "Monthly historical funding-rate archives.",
    ),
    "markPriceKlines": DatasetSpec(
        "markPriceKlines", "Mark-price klines", True, True, True, True,
        "Historical mark-price candles.",
    ),
    "indexPriceKlines": DatasetSpec(
        "indexPriceKlines", "Index-price klines", True, True, True, True,
        "Historical index-price candles.",
    ),
    "premiumIndexKlines": DatasetSpec(
        "premiumIndexKlines", "Premium-index klines", True, True, True, True,
        "Historical perpetual premium-index candles.",
    ),
    "aggTrades": DatasetSpec(
        "aggTrades", "Aggregate trades", True, True, False, False,
        "Compressed trade stream. Large for active symbols.",
    ),
    "trades": DatasetSpec(
        "trades", "Raw trades", True, True, False, False,
        "Individual trades. Very large for active symbols.",
    ),
    "bookDepth": DatasetSpec(
        "bookDepth", "Book-depth snapshots", False, True, False, False,
        "Daily depth snapshots where Binance publishes them.",
    ),
    "bookTicker": DatasetSpec(
        "bookTicker", "Book ticker", False, True, False, False,
        "Daily best bid/ask history. Binance archive coverage is not continuous to present.",
    ),
}

INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo")
DEFAULT_INTERVALS = ("1m", "5m", "1h", "4h", "1d")


def research_core_keys() -> list[str]:
    return [key for key, spec in DATASETS.items() if spec.research_core]
