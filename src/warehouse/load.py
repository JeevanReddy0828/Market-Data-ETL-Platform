from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def upsert_dim_security(engine: Engine, symbols: list[str]) -> None:
    if not symbols:
        return
    with engine.begin() as conn:
        for s in symbols:
            conn.execute(
                text("INSERT INTO dim_security(symbol) VALUES (:s) ON CONFLICT(symbol) DO NOTHING"),
                {"s": s},
            )


def load_prices(engine: Engine, prices: pd.DataFrame) -> int:
    if prices.empty:
        return 0
    df = prices.copy()
    df = df.rename(columns={"date": "trading_date"})
    # to_sql won't upsert; we do row-wise upsert for correctness (ok for demo).
    # For large scale: use COPY into staging + MERGE.
    cols = ["symbol", "trading_date", "open", "high", "low", "close", "volume"]
    rows = df[cols].to_dict(orient="records")
    sql = text("""
        INSERT INTO fact_prices_daily(symbol, trading_date, open, high, low, close, volume)
        VALUES (:symbol, :trading_date, :open, :high, :low, :close, :volume)
        ON CONFLICT(symbol, trading_date) DO UPDATE SET
          open=EXCLUDED.open,
          high=EXCLUDED.high,
          low=EXCLUDED.low,
          close=EXCLUDED.close,
          volume=EXCLUDED.volume,
          ingested_at=NOW()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def load_returns(engine: Engine, returns: pd.DataFrame) -> int:
    if returns.empty:
        return 0
    rows = returns.to_dict(orient="records")
    sql = text("""
        INSERT INTO fact_returns_daily(symbol, trading_date, daily_return)
        VALUES (:symbol, :trading_date, :daily_return)
        ON CONFLICT(symbol, trading_date) DO UPDATE SET
          daily_return=EXCLUDED.daily_return
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def load_vol(engine: Engine, vol: pd.DataFrame) -> int:
    if vol.empty:
        return 0
    rows = vol.to_dict(orient="records")
    sql = text("""
        INSERT INTO fact_volatility_30d(symbol, trading_date, vol_30d)
        VALUES (:symbol, :trading_date, :vol_30d)
        ON CONFLICT(symbol, trading_date) DO UPDATE SET
          vol_30d=EXCLUDED.vol_30d
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)
