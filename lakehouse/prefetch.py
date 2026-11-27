"""
Resolve and cache every JVM dependency (Delta, Kafka connector, hadoop-aws) at
Docker build time, so containers start without downloading jars.
"""

from lakehouse.config import load_settings
from lakehouse.spark_session import build_spark

if __name__ == "__main__":
    spark = build_spark("prefetch", load_settings(), kafka=True, s3=True, master="local[1]")
    spark.range(1).count()
    spark.stop()
    print("Spark dependencies cached")
