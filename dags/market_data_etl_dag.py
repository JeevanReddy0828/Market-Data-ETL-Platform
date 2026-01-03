from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# We mount project code at /opt/airflow/src in docker-compose.
from src.pipeline import run_pipeline_for_date


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="market_data_etl_daily",
    description="Daily market data ETL (Stooq -> validate -> transform -> Postgres) orchestrated by Airflow",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "market-data", "python", "sql"],
) as dag:
    start = EmptyOperator(task_id="start")

    def _run_for_execution_date(**context):
        # Airflow's logical date: we process the prior day by convention if desired.
        logical_date = context["logical_date"].date()
        run_pipeline_for_date(process_date=logical_date)

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=_run_for_execution_date,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> etl >> end
