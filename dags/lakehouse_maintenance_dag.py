"""
lakehouse_maintenance
=====================
Nightly Delta Lake housekeeping: compact the many small files that streaming
micro-batches create (OPTIMIZE) and remove files no longer referenced by any
table version older than the retention window (VACUUM).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/opt/pipeline")
JOBS_PYTHON = os.getenv("JOBS_PYTHON", "/opt/jobs-venv/bin/python")

with DAG(
    dag_id="lakehouse_maintenance",
    description="OPTIMIZE + VACUUM the Delta Lake tables",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "delta", "maintenance"],
) as dag:
    BashOperator(
        task_id="compact_and_vacuum",
        bash_command=f"cd {PIPELINE_DIR} && {JOBS_PYTHON} -m lakehouse.jobs.maintenance --retain-hours 168",
    )
