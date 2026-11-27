"""
Batch reconciliation of the Gold layer from Silver.

The streaming Gold query drops records that arrive after the watermark. Silver
keeps every valid event (flagged `is_late`), so this job recomputes the last N
hours from Silver and upserts the results. That makes Gold *eventually exact*
even for very late data, and also builds the hourly conversion funnel, which
needs distinct counts that streaming aggregations do not support.

    python -m lakehouse.jobs.rebuild_gold --lookback-hours 6
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import functions as F

from lakehouse import delta_io
from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark
from lakehouse.transformations import aggregate_funnel, aggregate_revenue

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("lakehouse.rebuild_gold")


def rebuild(spark, settings, lookback_hours: int) -> dict:
    silver = spark.read.format("delta").load(settings.silver_path)
    recent = silver.filter(
        F.col("event_ts") >= F.expr(f"current_timestamp() - INTERVAL {int(lookback_hours)} HOURS")
    )

    revenue = aggregate_revenue(recent, settings.window_duration).persist()
    funnel = aggregate_funnel(recent, "1 hour").persist()
    try:
        delta_io.ensure_table(
            spark, settings.gold_revenue_path, revenue, ["window_date"]
        )
        delta_io.ensure_table(spark, settings.gold_funnel_path, funnel, ["window_date"])

        revenue_rows = revenue.count()
        funnel_rows = funnel.count()
        if revenue_rows:
            delta_io.upsert(
                spark, revenue, settings.gold_revenue_path, ["window_start", "category"]
            )
        if funnel_rows:
            delta_io.upsert(
                spark, funnel, settings.gold_funnel_path, ["window_start", "device_type"]
            )
    finally:
        revenue.unpersist()
        funnel.unpersist()

    result = {"revenue_windows_upserted": revenue_rows, "funnel_windows_upserted": funnel_rows}
    logger.info("Gold rebuild finished: %s", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute Gold tables from Silver.")
    parser.add_argument("--lookback-hours", type=int, default=6)
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark("ecommerce-gold-rebuild", settings, s3=settings.is_s3)
    try:
        rebuild(spark, settings, args.lookback_hours)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
