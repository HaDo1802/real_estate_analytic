# Match your compose tag
FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
# Airflow’s official constraints pin compatible versions for providers & deps
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /tmp/requirements.txt
# Use constraints to keep Airflow deps happy while installing providers/psycopg2/etc.
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"
