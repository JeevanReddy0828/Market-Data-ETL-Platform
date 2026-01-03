from __future__ import annotations

from datetime import datetime
from sqlalchemy import text
from sqlalchemy.engine import Engine


def start_run(engine: Engine, run_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_run_audit(run_id, started_at, status)
                VALUES (:run_id, :started_at, 'RUNNING')
                ON CONFLICT(run_id) DO NOTHING
                """
            ),
            {"run_id": run_id, "started_at": datetime.utcnow()},
        )


def finish_run(
    engine: Engine,
    run_id: str,
    status: str,
    *,
    symbols: int,
    extracted_rows: int,
    loaded_prices: int,
    dq_null_violations: int,
    dq_duplicate_violations: int,
    dq_nonpositive_price: int,
    message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl_run_audit
                SET finished_at=:finished_at,
                    status=:status,
                    symbols=:symbols,
                    extracted_rows=:extracted_rows,
                    loaded_prices=:loaded_prices,
                    dq_null_violations=:dq_null_violations,
                    dq_duplicate_violations=:dq_duplicate_violations,
                    dq_nonpositive_price=:dq_nonpositive_price,
                    message=:message
                WHERE run_id=:run_id
                """
            ),
            {
                "run_id": run_id,
                "finished_at": datetime.utcnow(),
                "status": status,
                "symbols": symbols,
                "extracted_rows": extracted_rows,
                "loaded_prices": loaded_prices,
                "dq_null_violations": dq_null_violations,
                "dq_duplicate_violations": dq_duplicate_violations,
                "dq_nonpositive_price": dq_nonpositive_price,
                "message": message,
            },
        )
