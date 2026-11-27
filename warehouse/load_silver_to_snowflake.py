"""
Load the Silver Delta table into Snowflake's RAW_ECOMMERCE_EVENTS table.

    python -m warehouse.load_silver_to_snowflake --lookback-days 2

Design
------
* The Snowflake table keeps the exact shape the dbt staging models already read
  (raw_event VARIANT, kafka_partition, kafka_offset, ingested_at), so dbt is
  unchanged whether RAW is fed from Silver (this job) or directly from Kafka
  (ingestion/consumer_snowflake.py).
* Idempotent: rows are first written to a TEMPORARY stage table, then inserted
  into RAW only when their event_id is not already there. Re-running the job
  (or overlapping lookback windows) never creates duplicates.
* Reads only the last N days of Silver partitions (partition pruning on
  event_date), so each Airflow run stays cheap.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Iterable, Iterator, Sequence, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("warehouse.loader")

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_ROLE",
]

Row = Tuple[str, str, int, int]  # event_id, raw_event JSON, kafka_partition, kafka_offset
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def snowflake_configured() -> bool:
    """True when every SNOWFLAKE_* variable is set (used by Airflow to skip warehouse tasks)."""
    return all(os.getenv(v) for v in REQUIRED_ENV_VARS)


def _ident(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe Snowflake identifier: {name!r}")
    return name


def qualified_table(table: str = "RAW_ECOMMERCE_EVENTS") -> str:
    return ".".join(
        _ident(p) for p in (os.environ["SNOWFLAKE_DATABASE"], os.environ["SNOWFLAKE_SCHEMA"], table)
    )


def get_connection():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        client_session_keep_alive=True,
    )


# --------------------------------------------------------------------------- #
# SQL builders (pure functions, unit-tested)
# --------------------------------------------------------------------------- #
def ddl_statements(raw_table: str) -> list:
    db, schema, _ = raw_table.split(".")
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}",
        f"""CREATE TABLE IF NOT EXISTS {raw_table} (
            raw_event        VARIANT,
            kafka_partition  NUMBER,
            kafka_offset     NUMBER,
            ingested_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )""",
    ]


STAGE_DDL = """CREATE OR REPLACE TEMPORARY TABLE ECOMMERCE_EVENTS_STAGE (
    event_id         STRING,
    raw_event        VARIANT,
    kafka_partition  NUMBER,
    kafka_offset     NUMBER
)"""


def stage_insert_sql(n_rows: int) -> str:
    values = ", ".join(["(%s, %s, %s, %s)"] * n_rows)
    return (
        "INSERT INTO ECOMMERCE_EVENTS_STAGE (event_id, raw_event, kafka_partition, kafka_offset) "
        f"SELECT column1, PARSE_JSON(column2), column3, column4 FROM VALUES {values}"
    )


def publish_sql(raw_table: str) -> str:
    return f"""
        INSERT INTO {raw_table} (raw_event, kafka_partition, kafka_offset)
        SELECT s.raw_event, s.kafka_partition, s.kafka_offset
        FROM ECOMMERCE_EVENTS_STAGE s
        WHERE NOT EXISTS (
            SELECT 1 FROM {raw_table} r
            WHERE r.raw_event:event_id::string = s.event_id
        )
    """


def _chunks(rows: Iterable[Row], size: int) -> Iterator[Sequence[Row]]:
    chunk: list = []
    for row in rows:
        chunk.append(row)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def load_rows(conn, raw_table: str, rows: Iterable[Row], batch_size: int = 500) -> dict:
    """Stage `rows`, then insert the ones Snowflake has not seen. Returns counts."""
    staged = 0
    with conn.cursor() as cur:
        for statement in ddl_statements(raw_table):
            cur.execute(statement)
        cur.execute(STAGE_DDL)

        for chunk in _chunks(rows, batch_size):
            params: list = []
            for event_id, raw_event, partition, offset in chunk:
                params.extend([event_id, raw_event, partition, offset])
            cur.execute(stage_insert_sql(len(chunk)), params)
            staged += len(chunk)

        inserted = 0
        if staged:
            cur.execute(publish_sql(raw_table))
            inserted = cur.rowcount if cur.rowcount is not None else 0

    conn.commit()
    return {"staged": staged, "inserted": inserted, "already_present": staged - inserted}


def silver_rows(spark, settings, lookback_days: int, max_rows: int) -> Iterator[Row]:
    from pyspark.sql import functions as F

    from lakehouse.transformations import silver_to_raw_event_json

    silver = (
        spark.read.format("delta")
        .load(settings.silver_path)
        .filter(F.col("event_date") >= F.date_sub(F.current_date(), lookback_days))
        .orderBy("event_ts")
        .limit(max_rows)
    )
    for r in silver_to_raw_event_json(silver).toLocalIterator():
        yield (r["event_id"], r["raw_event"], r["kafka_partition"], r["kafka_offset"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Silver (Delta) into Snowflake RAW.")
    parser.add_argument("--lookback-days", type=int, default=2)
    parser.add_argument("--max-rows", type=int, default=500_000)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if not snowflake_configured():
        missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
        logger.error("Missing Snowflake environment variables: %s", ", ".join(missing))
        return 1

    from lakehouse.config import load_settings
    from lakehouse.spark_session import build_spark

    settings = load_settings()
    spark = build_spark("ecommerce-snowflake-loader", settings, s3=settings.is_s3)
    conn = get_connection()
    try:
        result = load_rows(
            conn,
            qualified_table(),
            silver_rows(spark, settings, args.lookback_days, args.max_rows),
            args.batch_size,
        )
        logger.info("Snowflake load finished: %s", result)
    finally:
        conn.close()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
