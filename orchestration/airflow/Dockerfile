FROM apache/airflow:3.4.0-python3.12

# The pipeline gets its own virtualenv so dbt's dependencies never clash with Airflow's.
USER root
RUN mkdir -p /opt/bank && chown airflow: /opt/bank
USER airflow

WORKDIR /opt/bank
COPY --chown=airflow: pyproject.toml README.md ./
COPY --chown=airflow: pipeline ./pipeline
COPY --chown=airflow: transform ./transform

RUN python -m venv /opt/bank/.venv \
    && /opt/bank/.venv/bin/pip install --no-cache-dir .

ENV BANK_PYTHON=/opt/bank/.venv/bin/python \
    BANK_HOME=/opt/bank \
    BANK_DATA_DIR=/opt/bank/data \
    BANK_WAREHOUSE=/opt/bank/data/warehouse.duckdb

WORKDIR /opt/airflow
