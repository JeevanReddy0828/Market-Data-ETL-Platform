# Market Data ETL Platform (Airflow + Python + SQL)

A production-style **batch ETL pipeline** that ingests daily market data, validates it, transforms it into analytics-ready tables, and loads it into a PostgreSQL warehouse — orchestrated by **Apache Airflow**.

This is designed to match data-engineering roles that focus on:
- building and maintaining scalable ETL pipelines
- Python + SQL transformations
- orchestration (Airflow)
- robust data quality checks + audit logging
- Linux/Docker-based workflows

---

## Architecture

```
Market Data Source
        |
        v
+--------------------+
| Ingestion Layer    |
| (Python)           |
+--------------------+
        |
        v
+--------------------+
| Validation Layer   |
| (Schema & DQ)      |
+--------------------+
        |
        v
+--------------------+
| Transformations    |
| (Returns, Vol)     |
+--------------------+
        |
        v
+--------------------+
| PostgreSQL         |
| Data Warehouse     |
+--------------------+

Orchestrated via Apache Airflow
Containerized with Docker
```

---

## Technology Stack

| Layer           | Technologies           |
| --------------- | ---------------------- |
| Orchestration   | Apache Airflow         |
| Language        | Python, SQL            |
| Data Processing | Pandas                 |
| Warehouse       | PostgreSQL             |
| Infrastructure  | Docker, Docker Compose |
| OS              | Linux                  |

---

## Data Model

### Dimension Tables

* `dim_security` – unique financial instruments

### Fact Tables

* `fact_prices_daily` – daily OHLCV prices
* `fact_returns_daily` – daily returns
* `fact_volatility_30d` – rolling volatility metrics

### Operational Tables

* `etl_run_audit` – pipeline execution metadata and data quality metrics

---

## Airflow DAG

**DAG Name:** `market_data_etl_daily`
**Schedule:** `@daily`
**Catchup:** Disabled
**Retries:** Enabled

Each DAG run performs:

1. Data extraction
2. Validation and cleansing
3. Transformation
4. Warehouse loading
5. Audit logging

---

## Project Structure

```
Market-Data-ETL-Platform/
├── dags/
│   └── market_data_etl_dag.py
├── src/
│   ├── ingestion/
│   ├── staging/
│   ├── transforms/
│   ├── warehouse/
│   └── quality/
├── sql/
│   └── 001_create_tables.sql
├── tests/
├── docker-compose.yml
├── Dockerfile
└── README.md
```

---

## Notes
- Stooq symbols use the format like `aapl.us`, `msft.us`, `tsla.us`.
- If Stooq is blocked in your network, you can switch to the synthetic generator in `src/ingestion/synthetic.py`.

