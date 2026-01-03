from __future__ import annotations

import pandas as pd


def compute_daily_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """prices: symbol, date, close"""
    df = prices[["symbol", "date", "close"]].copy()
    df = df.sort_values(["symbol", "date"])
    df["daily_return"] = df.groupby("symbol")["close"].pct_change()
    out = df.dropna(subset=["daily_return"])
    out = out.rename(columns={"date": "trading_date"})
    return out[["symbol", "trading_date", "daily_return"]]


def compute_rolling_vol(prices: pd.DataFrame, window: int = 30) -> pd.DataFrame:
    df = prices[["symbol", "date", "close"]].copy()
    df = df.sort_values(["symbol", "date"])
    df["ret"] = df.groupby("symbol")["close"].pct_change()
    df["vol_30d"] = (
        df.groupby("symbol")["ret"]
        .rolling(window=window, min_periods=window)
        .std()
        .reset_index(level=0, drop=True)
    )
    out = df.dropna(subset=["vol_30d"]).rename(columns={"date": "trading_date"})
    return out[["symbol", "trading_date", "vol_30d"]]
