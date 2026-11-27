"""
Entry point of the streaming container.

    python -m lakehouse.run_streaming

Starts the Bronze, Silver and Gold queries in one Spark application and blocks
until one of them fails or the process receives SIGTERM/SIGINT. On failure the
process exits non-zero and Docker restarts it; checkpoints make it resume
exactly where it stopped.
"""

from __future__ import annotations

import logging
import signal
import sys

from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark
from lakehouse.storage import ensure_bucket
from lakehouse.streams import bootstrap_tables, start_bronze, start_gold_revenue, start_silver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("lakehouse.run")


def main() -> int:
    settings = load_settings()
    logger.info(
        "Starting lakehouse streaming: kafka=%s topic=%s lake=%s endpoint=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.base_path,
        settings.s3_endpoint or "AWS S3",
    )

    ensure_bucket(settings)
    spark = build_spark("ecommerce-lakehouse-streaming", settings, kafka=True, s3=settings.is_s3)
    bootstrap_tables(spark, settings)

    queries = [
        start_bronze(spark, settings),
        start_silver(spark, settings),
        start_gold_revenue(spark, settings),
    ]
    for q in queries:
        logger.info("Started query '%s' (id=%s)", q.name, q.id)

    def _shutdown(signum, _frame):
        logger.info("Signal %s received, stopping queries...", signum)
        for q in queries:
            if q.isActive:
                q.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
    logger.info("Streaming stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
