"""SparkSession factory: Delta Lake + (optionally) Kafka source + S3A/MinIO."""

from __future__ import annotations

import os
from typing import Iterable, Optional

from pyspark.sql import SparkSession

from lakehouse.config import Settings, load_settings

SPARK_VERSION = "3.5.3"
SCALA_SUFFIX = "2.12"
# hadoop-aws must match the Hadoop client bundled with the pyspark wheel (3.3.4).
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_SUFFIX}:{SPARK_VERSION}"


def build_spark(
    app_name: str,
    settings: Optional[Settings] = None,
    *,
    kafka: bool = False,
    s3: bool = True,
    master: Optional[str] = None,
    extra_packages: Iterable[str] = (),
    shuffle_partitions: int = 4,
) -> SparkSession:
    """
    Build a SparkSession with the Delta Lake extensions enabled.

    kafka=True adds the Kafka connector; s3=True adds hadoop-aws and the S3A
    settings (MinIO endpoint when S3_ENDPOINT is set, AWS S3 otherwise).
    Jars are resolved through Ivy (pre-fetched inside the Docker images).
    """
    from delta import configure_spark_with_delta_pip

    settings = settings or load_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.metricsEnabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1536m"))
    )

    ivy_dir = os.getenv("SPARK_IVY_DIR")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    packages = list(extra_packages)
    if kafka:
        packages.append(KAFKA_PACKAGE)

    if s3:
        packages.append(HADOOP_AWS)
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        if settings.s3_endpoint:  # MinIO / any S3-compatible store
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.connection.ssl.enabled",
                    "true" if settings.s3_endpoint.startswith("https") else "false",
                )
            )
        if settings.aws_access_key_id and settings.aws_secret_access_key:
            builder = (
                builder.config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
                .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
            )
        else:  # IAM role / instance profile / env credentials on real AWS
            builder = builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )

    extra = os.getenv("SPARK_EXTRA_PACKAGES")
    if extra:
        packages.extend(p.strip() for p in extra.split(",") if p.strip())

    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
