from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from io import StringIO

import pandas as pd
import requests


@dataclass(frozen=True)
class StooqClient:
    timeout_s: int = 20

    def fetch_daily(self, symbol: str) -> pd.DataFrame:
        # Example: https://stooq.com/q/d/l/?s=aapl.us&i=d
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        resp = requests.get(url, timeout=self.timeout_s)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        # Standardize
        df.columns = [c.strip().lower() for c in df.columns]
        # Expected columns: date, open, high, low, close, volume
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["symbol"] = symbol
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    def filter_date(self, df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
        m = (df["date"] >= start) & (df["date"] <= end)
        return df.loc[m].copy()
