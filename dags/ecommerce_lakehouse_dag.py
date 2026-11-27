"""
ecommerce_lakehouse_pipeline
============================
Orchestrates everything that is *not* the always-on Spark streaming job:

    validate_silver ─► rebuild_gold ─► validate_gold
          │
          └► check_snowflake ─► load_snowflake ─► dbt_deps ─► dbt_run ─► dbt_test

* validate_silver / validate_gold : Great Expectations suites; a failure stops the
  DAG before bad data can reach Snowflake.
* rebuild_gold                    : batch reconciliation that folds records which
  arrived after the streaming watermark back into the Gold tables.
* check_snowflake                 : skips the warehouse branch (instead of failing) on
  machines without Snowflake credentials, so the lakehouse half of the DAG
  still runs green locally.
* load_snowflake / dbt_*          : Silver -> Snowflake RAW, then dbt ELT into the
  dimensional models and marts, then dbt tests.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/opt/pipeline")
JOBS_PYTHON = os.getenv("JOBS_PYTHON", "/opt/jobs-venv/bin/python")
DBT_BIN = os.getenv("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_WORKDIR = "/tmp/dbt_project"  # writable copy: dbt writes target/ and dbt_packages/

REQUIRED_SNOWFLAKE_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_ROLE",
]


def snowflake_is_configured() -> bool:
    missing = [v for v in REQUIRED_SNOWFLAKE_VARS if not os.getenv(v)]
    if missing:
        print(f"Snowflake not configured (missing: {', '.join(missing)}); skipping warehouse tasks.")
        return False
    return True


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ecommerce_lakehouse_pipeline",
    description="Quality gates, Gold reconciliation and Snowflake/dbt ELT for the e-commerce lakehouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "delta", "snowflake", "dbt"],
) as dag:

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"cd {PIPELINE_DIR} && {JOBS_PYTHON} -m quality.validate_lakehouse --layer silver",
    )

    rebuild_gold = BashOperator(
        task_id="rebuild_gold",
        bash_command=f"cd {PIPELINE_DIR} && {JOBS_PYTHON} -m lakehouse.jobs.rebuild_gold --lookback-hours 6",
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=f"cd {PIPELINE_DIR} && {JOBS_PYTHON} -m quality.validate_lakehouse --layer gold --lookback-hours 6",
    )

    check_snowflake = ShortCircuitOperator(
        task_id="check_snowflake_configured",
        python_callable=snowflake_is_configured,
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=f"cd {PIPELINE_DIR} && {JOBS_PYTHON} -m warehouse.load_silver_to_snowflake --lookback-days 2",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"rm -rf {DBT_WORKDIR} && cp -r {PIPELINE_DIR}/dbt_transformations {DBT_WORKDIR} "
            f"&& cd {DBT_WORKDIR} && {DBT_BIN} deps"
        ),
        env={"DBT_PROFILES_DIR": DBT_WORKDIR},
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_WORKDIR} && {DBT_BIN} run",
        env={"DBT_PROFILES_DIR": DBT_WORKDIR},
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_WORKDIR} && {DBT_BIN} test",
        env={"DBT_PROFILES_DIR": DBT_WORKDIR},
        append_env=True,
    )

    validate_silver >> rebuild_gold >> validate_gold
    validate_silver >> check_snowflake >> load_snowflake >> dbt_deps >> dbt_run >> dbt_test
