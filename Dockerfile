FROM apache/airflow:2.9.3-python3.11

# Install python deps as airflow user (Airflow images block pip as root)
USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    PyYAML \
    SQLAlchemy \
    psycopg2-binary \
    requests \
    python-dateutil
