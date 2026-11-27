"""Delta table maintenance: compact small files and vacuum old versions."""

from __future__ import annotations

import argparse
import logging
import sys

from lakehouse import delta_io
from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("lakehouse.maintenance")


def main() -> int:
    parser = argparse.ArgumentParser(description="OPTIMIZE + VACUUM the lakehouse tables.")
    parser.add_argument("--retain-hours", type=int, default=168)
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark("ecommerce-lakehouse-maintenance", settings, s3=settings.is_s3)
    try:
        from delta.tables import DeltaTable

        for path in (
            settings.bronze_path,
            settings.silver_path,
            settings.quarantine_path,
            settings.gold_revenue_path,
            settings.gold_funnel_path,
        ):
            if DeltaTable.isDeltaTable(spark, path):
                logger.info("Compacting %s", path)
                delta_io.compact_and_vacuum(spark, path, args.retain_hours)
            else:
                logger.info("Skipping %s (not created yet)", path)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
