from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SyntheticMarketGenerator:
    seed: int = 42

    def generate(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        rng = np.random.default_rng(self.seed + abs(hash(symbol)) % 10_000)
        days = (end - start).days + 1
        dates = [start + timedelta(days=i) for i in range(days)]
        # remove weekends for realism
        dates = [d for d in dates if d.weekday() < 5]
        n = len(dates)
        base = rng.uniform(50, 500)
        rets = rng.normal(0, 0.015, size=n)
        price = base * np.exp(np.cumsum(rets))
        close = price
        open_ = close * (1 + rng.normal(0, 0.002, size=n))
        high = np.maximum(open_, close) * (1 + rng.uniform(0, 0.01, size=n))
        low = np.minimum(open_, close) * (1 - rng.uniform(0, 0.01, size=n))
        vol = rng.integers(100_000, 5_000_000, size=n)

        df = pd.DataFrame(
            {
                "symbol": symbol,
                "date": dates,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": vol,
            }
        )
        return df
