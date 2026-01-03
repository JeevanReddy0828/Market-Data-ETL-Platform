# Market Data ETL Platform

**Apache Airflow | Python | SQL | PostgreSQL | Docker**

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9+-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)

---

## Overview

The **Market Data ETL Platform** is a production-grade data engineering system that ingests, validates, transforms, and warehouses daily financial market data. The pipeline is orchestrated using **Apache Airflow**, containerized with **Docker**, and implemented in **Python and SQL** following enterprise data engineering best practices.

This project demonstrates how modern data teams design **reliable, observable, and reproducible ETL pipelines** to support analytics, quantitative research, and downstream applications.

---

## Key Capabilities

| Capability               | Description                                              |
| ------------------------ | -------------------------------------------------------- |
| **ETL Orchestration**    | End-to-end workflow orchestration using Apache Airflow   |
| **Data Validation**      | Schema checks, duplicate detection, and value validation |
| **Analytical Modeling**  | Fact and dimension tables optimized for analytics        |
| **Observability**        | Audit logging for each DAG run with execution metrics    |
| **Containerization**     | Fully Dockerized stack for reproducible environments     |
| **Production Practices** | Idempotent loads, retries, failure handling              |

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

## Running Locally

### Prerequisites

* Docker Desktop
* Docker Compose

### Start the stack

```bash
docker compose up --build
```

### Access Airflow

```
http://localhost:8080
Username: airflow
Password: airflow
```

Trigger the `market_data_etl_daily` DAG from the UI.

---

## Observability & Auditing

Each pipeline run records:

* rows extracted and loaded
* data quality violations
* execution timestamps
* final status (SUCCESS / FAILED)

This enables traceability, debugging, and production monitoring.

---

## Production Readiness Highlights

* Idempotent warehouse loads
* Explicit schema validation
* Retry and failure handling
* Modular, testable codebase
* Dockerized Linux execution environment

---

## Planned Extensions (Roadmap)

### Apache Spark (Distributed Processing)

* Replace Pandas transforms with PySpark
* Enable horizontal scaling for large datasets

### Cloud Data Lake (Amazon S3)

* Raw and curated datasets stored in S3
* Partitioned by symbol and date

### Snowflake Warehouse

* Cloud-native analytics warehouse
* COPY-based ingestion from S3 stages

These extensions are **designed and documented**, and can be enabled without changing the core Airflow orchestration.

---

## Resume Summary (How to Describe This Project)

> Built and deployed a production-grade ETL platform using Apache Airflow, Python, SQL, PostgreSQL, and Docker, featuring data validation, audit logging, and analytics-ready warehouse modeling.

---

## Author

**Jeevan Reddy Arlagadda**
Software Engineer | Data Engineering