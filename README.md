# Market Data ETL Platform (Airflow + Python + SQL)

A production-style **batch ETL pipeline** that ingests daily market data, validates it, transforms it into analytics-ready tables, and loads it into a PostgreSQL warehouse orchestrated by **Apache Airflow**.

This is designed to match data-engineering roles that focus on:
- building and maintaining scalable ETL pipelines
- Python + SQL transformations
- orchestration (Airflow)
- robust data quality checks + audit logging
- Linux/Docker-based workflows

---

## Architecture

**Extract**
- Pull OHLCV daily bars from **Stooq** (no API key) for a configurable symbol list.
- Write raw files to a local “data lake” style layout.

**Transform**
- Validate schemas and run quality checks (nulls, dupes, non-positive prices).
- Create curated tables: prices + returns + rolling volatility.

**Load**
- Load into PostgreSQL tables with indexing.

**Orchestrate**
- Airflow DAG runs the full pipeline with retries and run auditing.

---

## Quickstart (Docker)

### 1) Prereqs
- Docker Desktop
- Docker Compose

### 2) Start Postgres + Airflow
```bash
docker compose up --build
```

Airflow UI: http://localhost:8080  
Default creds: `airflow` / `airflow`

### 3) Trigger the DAG
DAG name: `market_data_etl_daily`

---

## Local (no Docker)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m src.cli run-once --start 2024-01-01 --end 2024-01-31
```

---

## Config

Edit `config/pipeline.yml`:
- symbols
- start/end dates
- storage paths

Environment variables (optional):
- `WAREHOUSE_URL` (SQLAlchemy URL, e.g. `postgresql+psycopg2://user:pass@host:5432/db`)
- `RAW_DIR`, `STAGED_DIR`

---

## Tables
- `dim_security`
- `fact_prices_daily`
- `fact_returns_daily`
- `fact_volatility_30d`
- `etl_run_audit` (run metrics + dq stats)

---

## Repo structure
```
.
├── dags/                         # Airflow DAG(s)
├── config/
├── sql/
├── src/
│   ├── ingestion/
│   ├── staging/
│   ├── transforms/
│   ├── warehouse/
│   ├── quality/
│   └── cli.py
├── tests/
└── docker-compose.yml
```


