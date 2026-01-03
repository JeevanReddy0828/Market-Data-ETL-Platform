import pandas as pd
from datetime import date

from src.transforms.models import compute_daily_returns


def test_compute_daily_returns_basic():
    df = pd.DataFrame(
        {
            "symbol": ["aapl.us", "aapl.us", "aapl.us"],
            "date": [date(2024,1,1), date(2024,1,2), date(2024,1,3)],
            "close": [100.0, 110.0, 99.0],
            "open": [0,0,0],
            "high": [0,0,0],
            "low": [0,0,0],
            "volume": [1,1,1],
        }
    )
    out = compute_daily_returns(df)
    assert len(out) == 2
    assert out.iloc[0]["daily_return"] == 0.10
