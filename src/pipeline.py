from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from src.ingestion.stooq import StooqClient
from src.ingestion.synthetic import SyntheticMarketGenerator
from src.settings import load_settings
from src.staging.validate import clean_prices, validate_prices
from src.transforms.models import compute_daily_returns, compute_rolling_vol
from src.utils import ensure_dir, sha1_text
from src.warehouse.db import get_engine, run_sql
from src.warehouse.load import (
    load_prices,
    load_returns,
    load_vol,
    upsert_dim_security,
)
from src.quality.audit import start_run, finish_run


def _raw_path(raw_dir: Path, symbol: str, process_date: date) -> Path:
    return raw_dir / f"symbol={symbol}" / f"dt={process_date.isoformat()}.parquet"


def _stage_path(staged_dir: Path, process_date: date) -> Path:
    return staged_dir / f"dt={process_date.isoformat()}.parquet"


def run_pipeline_for_date(process_date: date) -> None:
    settings = load_settings()

    raw_dir = settings.storage.raw_dir
    staged_dir = settings.storage.staged_dir
    ensure_dir(raw_dir)
    ensure_dir(staged_dir)

    engine = get_engine(settings.warehouse.url)

    # Create tables if needed
    sql_path = Path("sql/001_create_tables.sql")
    run_sql(engine, sql_path.read_text(encoding="utf-8"))

    run_id = sha1_text(f"{process_date.isoformat()}|{','.join(settings.pipeline.symbols)}")
    start_run(engine, run_id)

    extracted_rows = 0
    dq_null = dq_dupe = dq_nonpos = 0
    loaded_prices = 0

    try:
        # 1) Extract
        client = StooqClient()
        synthetic = SyntheticMarketGenerator()

        all_frames: list[pd.DataFrame] = []
        for symbol in settings.pipeline.symbols:
            try:
                df = client.fetch_daily(symbol)
            except Exception:
                # fallback if network blocked
                df = synthetic.generate(symbol, process_date, process_date)
            df = df[df["date"] == process_date]
            if df.empty:
                continue

            extracted_rows += len(df)

            # write raw
            raw_p = _raw_path(raw_dir, symbol, process_date)
            ensure_dir(raw_p.parent)
            df.to_parquet(raw_p, index=False)

            all_frames.append(df)

        if not all_frames:
            finish_run(
                engine,
                run_id,
                "SUCCESS",
                symbols=len(settings.pipeline.symbols),
                extracted_rows=0,
                loaded_prices=0,
                dq_null_violations=0,
                dq_duplicate_violations=0,
                dq_nonpositive_price=0,
                message="No rows extracted for process_date",
            )
            return

        raw_all = pd.concat(all_frames, ignore_index=True)

        # 2) Validate + stage
        v = validate_prices(raw_all)
        dq_null += v["null_violations"]
        dq_dupe += v["duplicate_violations"]
        dq_nonpos += v["nonpositive_price"]

        staged = clean_prices(raw_all)
        stage_p = _stage_path(staged_dir, process_date)
        ensure_dir(stage_p.parent)
        staged.to_parquet(stage_p, index=False)

        # 3) Load prices
        upsert_dim_security(engine, sorted(staged["symbol"].unique().tolist()))
        loaded_prices = load_prices(engine, staged)

        # 4) Transforms (for current date, use warehouse history for better continuity)
        # For demo: compute transforms from staged only (single day).
        returns = compute_daily_returns(staged)
        vol = compute_rolling_vol(staged, window=30)

        load_returns(engine, returns)
        load_vol(engine, vol)

        finish_run(
            engine,
            run_id,
            "SUCCESS",
            symbols=len(settings.pipeline.symbols),
            extracted_rows=extracted_rows,
            loaded_prices=loaded_prices,
            dq_null_violations=dq_null,
            dq_duplicate_violations=dq_dupe,
            dq_nonpositive_price=dq_nonpos,
            message=None,
        )
    except Exception as e:
        finish_run(
            engine,
            run_id,
            "FAILED",
            symbols=len(settings.pipeline.symbols),
            extracted_rows=extracted_rows,
            loaded_prices=loaded_prices,
            dq_null_violations=dq_null,
            dq_duplicate_violations=dq_dupe,
            dq_nonpositive_price=dq_nonpos,
            message=str(e),
        )
        raise
