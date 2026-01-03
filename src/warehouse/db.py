from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine(warehouse_url: str) -> Engine:
    return create_engine(warehouse_url, pool_pre_ping=True)


def run_sql(engine: Engine, sql: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql))
