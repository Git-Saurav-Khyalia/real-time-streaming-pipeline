"""
consumer_snowflake.py
----------------------
Consumes JSON e-commerce events from Kafka (topic: ecommerce_events_raw) and
loads them in micro-batches into a Snowflake VARIANT staging table:
    STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS

Design notes:
- Manual offset commit: offsets are only committed to Kafka AFTER a batch has
  been durably written to Snowflake, giving at-least-once delivery into the
  warehouse (dbt/staging models de-duplicate on event_id downstream).
- Micro-batching: buffers up to BATCH_SIZE messages or FLUSH_INTERVAL_SECONDS
  (whichever comes first), then performs a single bulk INSERT using
  Snowflake's `INSERT INTO ... SELECT PARSE_JSON(column1) FROM VALUES (...)`
  pattern, which is fast for small/medium batches and avoids needing an
  external stage for a local dev pipeline.
- Malformed messages are routed to a dead-letter list and logged rather than
  crashing the batch.
- All connection settings are pulled from environment variables / .env so no
  secrets are hardcoded.

Snowflake target DDL (run once, see README.md "Snowflake Setup"):

    CREATE DATABASE IF NOT EXISTS STREAMING_DB;
    CREATE SCHEMA IF NOT EXISTS STREAMING_DB.RAW_SCHEMA;
    CREATE TABLE IF NOT EXISTS STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS (
        raw_event        VARIANT,
        kafka_partition   NUMBER,
        kafka_offset      NUMBER,
        ingested_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

Usage:
    python consumer_snowflake.py --topic ecommerce_events_raw \
        --bootstrap-servers localhost:9092 --batch-size 100 --flush-interval 5
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | consumer | %(message)s",
)
logger = logging.getLogger("consumer")

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_ROLE",
]


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)

    def _exit(self, *_args):
        self.kill_now = True


def validate_env() -> None:
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        logger.error("Copy .env.example to .env and fill in your Snowflake credentials.")
        sys.exit(1)


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
        client_session_keep_alive=True,
    )


def ensure_target_table(conn: snowflake.connector.SnowflakeConnection, table: str) -> None:
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            raw_event         VARIANT,
            kafka_partition    NUMBER,
            kafka_offset       NUMBER,
            ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    logger.info("Confirmed target table exists: %s", table)


def build_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,   # we commit manually after a successful Snowflake write
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer


def flush_batch(
    conn: snowflake.connector.SnowflakeConnection,
    table: str,
    batch: list,
) -> None:
    """Bulk-insert a batch of JSON events into Snowflake.

    JSON strings are passed through VALUES as plain VARCHAR values and
    converted to VARIANT using PARSE_JSON in the SELECT projection.
    """

    if not batch:
        return

    values_sql = ", ".join(
        ["(%s, %s, %s)"] * len(batch)
    )

    insert_sql = f"""
        INSERT INTO {table}
            (raw_event, kafka_partition, kafka_offset)
        SELECT
            PARSE_JSON(column1),
            column2,
            column3
        FROM VALUES {values_sql}
    """

    params = []

    for json_str, partition, offset in batch:
        params.extend([
            json_str,
            partition,
            offset,
        ])

    with conn.cursor() as cur:
        cur.execute("BEGIN")

        try:
            cur.execute(insert_sql, params)
            conn.commit()

        except Exception:
            conn.rollback()
            raise


def run(args: argparse.Namespace) -> None:
    validate_env()
    table = f'{os.environ["SNOWFLAKE_DATABASE"]}.{os.environ["SNOWFLAKE_SCHEMA"]}.{args.table}'

    conn = get_snowflake_connection()
    ensure_target_table(conn, table)

    consumer = build_consumer(args.bootstrap_servers, args.group_id, args.topic)
    killer = GracefulKiller()

    batch: list = []
    dead_letters: list = []
    last_flush = time.time()
    total_loaded = 0

    logger.info(
        "Starting consumer -> topic=%s group=%s batch_size=%d flush_interval=%ds",
        args.topic, args.group_id, args.batch_size, args.flush_interval,
    )

    try:
        while not killer.kill_now:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        pass
                    else:
                        raise KafkaException(msg.error())
                else:
                    raw_value = msg.value()
                    try:
                        # Validate it's well-formed JSON before shipping to Snowflake;
                        # PARSE_JSON would also catch this, but failing fast here lets
                        # us dead-letter without touching the warehouse.
                        json.loads(raw_value)
                        batch.append((raw_value.decode("utf-8"), msg.partition(), msg.offset()))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning("Dead-lettering malformed message at offset %s: %s",
                                       msg.offset(), e)
                        dead_letters.append(raw_value)

            should_flush = (
                len(batch) >= args.batch_size
                or (batch and (time.time() - last_flush) >= args.flush_interval)
            )

            if should_flush:
                try:
                    flush_batch(conn, table, batch)
                    consumer.commit(asynchronous=False)  # commit only after durable write
                    total_loaded += len(batch)
                    logger.info(
                        "Flushed %d events to Snowflake (total=%d, dead_letters=%d)",
                        len(batch), total_loaded, len(dead_letters),
                    )
                    batch = []
                    last_flush = time.time()
                except Exception:
                    logger.exception(
                        "Failed to flush batch of %d events to Snowflake; "
                        "will retry without committing offsets", len(batch)
                    )
                    time.sleep(2)  # backoff before retrying the same batch

    finally:
        if batch:
            logger.info("Flushing final partial batch of %d events before shutdown", len(batch))
            try:
                flush_batch(conn, table, batch)
                consumer.commit(asynchronous=False)
            except Exception:
                logger.exception("Final flush failed; these offsets were NOT committed")

        consumer.close()
        conn.close()
        logger.info("Consumer stopped cleanly. Total loaded=%d, dead_letters=%d",
                    total_loaded, len(dead_letters))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Kafka e-commerce events into Snowflake.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ecommerce_events_raw")
    parser.add_argument("--group-id", default="snowflake-loader-group")
    parser.add_argument("--table", default="RAW_ECOMMERCE_EVENTS",
                         help="Target table name (unqualified; DB/schema come from env vars)")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--flush-interval", type=int, default=5,
                         help="Max seconds to hold a partial batch before flushing")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception:
        logger.exception("Consumer crashed with an unhandled exception")
        sys.exit(1)
