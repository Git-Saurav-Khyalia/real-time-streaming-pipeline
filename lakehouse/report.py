"""
Print row counts for every lakehouse table (and optionally wait until the
pipeline has produced data). Used for smoke tests and by CI.

    python -m lakehouse.report
    python -m lakehouse.report --wait --timeout 600      # exit 0 once all layers have data
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import functions as F

from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark


def collect(spark, settings) -> dict:
    from delta.tables import DeltaTable

    tables = {
        "bronze": settings.bronze_path,
        "silver": settings.silver_path,
        "quarantine": settings.quarantine_path,
        "gold_revenue_per_minute": settings.gold_revenue_path,
        "gold_conversion_funnel_hourly": settings.gold_funnel_path,
    }
    counts = {}
    for name, path in tables.items():
        counts[name] = (
            spark.read.format("delta").load(path).count()
            if DeltaTable.isDeltaTable(spark, path)
            else None
        )
    if counts.get("silver"):
        silver = spark.read.format("delta").load(settings.silver_path)
        stats = silver.agg(
            F.countDistinct("event_id").alias("distinct_event_ids"),
            F.sum(F.col("is_late").cast("int")).alias("late_events"),
        ).first()
        counts["silver_distinct_event_ids"] = stats["distinct_event_ids"]
        counts["silver_late_events"] = stats["late_events"]
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait", action="store_true", help="poll until bronze/silver/gold have rows")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--interval", type=int, default=20)
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark("ecommerce-lakehouse-report", settings, s3=settings.is_s3)
    deadline = time.time() + args.timeout
    try:
        while True:
            counts = collect(spark, settings)
            print("Lakehouse row counts:")
            for name, value in counts.items():
                print(f"  {name:<32} {value}")
            ready = all(counts.get(k) for k in ("bronze", "silver", "gold_revenue_per_minute"))
            if not args.wait:
                return 0
            if ready:
                if counts["silver"] != counts["silver_distinct_event_ids"]:
                    print("ERROR: duplicate event_ids found in Silver", file=sys.stderr)
                    return 1
                print("OK: bronze, silver and gold all contain data and Silver has no duplicates")
                return 0
            if time.time() > deadline:
                print("ERROR: timed out waiting for the pipeline", file=sys.stderr)
                return 1
            time.sleep(args.interval)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
