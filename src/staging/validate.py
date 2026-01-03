from __future__ import annotations

import pandas as pd


REQUIRED_COLS = ["symbol", "date", "open", "high", "low", "close", "volume"]


def validate_prices(df: pd.DataFrame) -> dict[str, int]:
    """Return violation counts. Does not mutate df."""
    violations: dict[str, int] = {
        "null_violations": 0,
        "duplicate_violations": 0,
        "nonpositive_price": 0,
    }

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    violations["null_violations"] = int(df[REQUIRED_COLS].isna().any(axis=1).sum())

    dupes = df.duplicated(subset=["symbol", "date"]).sum()
    violations["duplicate_violations"] = int(dupes)

    price_cols = ["open", "high", "low", "close"]
    nonpos = (df[price_cols] <= 0).any(axis=1).sum()
    violations["nonpositive_price"] = int(nonpos)

    return violations


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.dropna(subset=["symbol", "date", "close"])
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last")
    # Ensure numeric
    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["close"])
    # Remove bad rows
    out = out[(out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0) & (out["close"] > 0)]
    out["volume"] = out["volume"].fillna(0).astype("int64")
    return out
