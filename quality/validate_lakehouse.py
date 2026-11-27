"""
Validate the lakehouse tables with Great Expectations and fail loudly.

    python -m quality.validate_lakehouse --layer silver
    python -m quality.validate_lakehouse --layer gold

Every run is appended to the Delta table `quality/validation_results`, giving a
queryable audit trail of data-quality over time. The exit code is 1 when any
expectation fails, which makes the Airflow task (and therefore the DAG) fail
before bad data reaches Snowflake.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark
from quality.expectations import SUITES
from quality.runner import validate_by_name

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("quality")

LAYERS = {
    "silver": ["silver_events"],
    "gold": ["gold_revenue_per_minute", "gold_conversion_funnel_hourly"],
}


def _load(spark, settings, suite: str, lookback_hours: int, max_rows: int):
    paths = {
        "silver_events": settings.silver_path,
        "gold_revenue_per_minute": settings.gold_revenue_path,
        "gold_conversion_funnel_hourly": settings.gold_funnel_path,
    }
    df = spark.read.format("delta").load(paths[suite])
    ts_col = "event_ts" if suite == "silver_events" else "window_start"
    cutoff = F.expr(f"current_timestamp() - INTERVAL {int(lookback_hours)} HOURS")
    return df.filter(F.col(ts_col) >= cutoff).limit(max_rows).toPandas()


def _record(spark, settings, summaries: list) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (
            now,
            s["suite"],
            s["success"],
            s["rows_validated"],
            s["evaluated_expectations"],
            s["successful_expectations"],
            json.dumps(s["failures"]),
        )
        for s in summaries
    ]
    spark.createDataFrame(
        rows,
        "run_at timestamp, suite string, success boolean, rows_validated long, "
        "evaluated_expectations long, successful_expectations long, failures_json string",
    ).write.format("delta").mode("append").save(settings.validation_results_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Great Expectations validation of the lakehouse.")
    parser.add_argument("--layer", choices=sorted(LAYERS), required=True)
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--max-rows", type=int, default=200_000)
    parser.add_argument("--no-fail", action="store_true", help="always exit 0 (report only)")
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark("ecommerce-quality", settings, s3=settings.is_s3)
    summaries = []
    try:
        for suite in LAYERS[args.layer]:
            pdf = _load(spark, settings, suite, args.lookback_hours, args.max_rows)
            summary = validate_by_name(suite, pdf, SUITES)
            summaries.append(summary)
            status = "PASSED" if summary["success"] else "FAILED"
            logger.info(
                "%s %s: %d/%d expectations on %d rows",
                suite, status, summary["successful_expectations"],
                summary["evaluated_expectations"], summary["rows_validated"],
            )
            for failure in summary["failures"]:
                logger.error("  failed: %s", failure)
        _record(spark, settings, summaries)
    finally:
        spark.stop()

    ok = all(s["success"] for s in summaries)
    return 0 if ok or args.no_fail else 1


if __name__ == "__main__":
    sys.exit(main())
