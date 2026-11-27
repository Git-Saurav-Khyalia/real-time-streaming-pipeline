# Airflow + isolated virtualenvs for the heavy tool chains (avoids dependency
# clashes between Airflow, dbt, Spark and Great Expectations).
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/jobs-venv /opt/dbt-venv /opt/pipeline /opt/ivy \
 && chown -R airflow /opt/jobs-venv /opt/dbt-venv /opt/pipeline /opt/ivy

USER airflow
ENV SPARK_IVY_DIR=/opt/ivy \
    PIPELINE_DIR=/opt/pipeline \
    JOBS_PYTHON=/opt/jobs-venv/bin/python \
    DBT_BIN=/opt/dbt-venv/bin/dbt

COPY --chown=airflow lakehouse/requirements.txt /tmp/jobs-requirements.txt
RUN python -m venv /opt/jobs-venv \
 && /opt/jobs-venv/bin/pip install --no-cache-dir -r /tmp/jobs-requirements.txt

RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-snowflake==1.12.3

# Cache Spark jars (the code itself is bind-mounted at runtime by docker-compose).
COPY --chown=airflow lakehouse /opt/pipeline/lakehouse
RUN cd /opt/pipeline && PYTHONPATH=/opt/pipeline /opt/jobs-venv/bin/python -m lakehouse.prefetch
