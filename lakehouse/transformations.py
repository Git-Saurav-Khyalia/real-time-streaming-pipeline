"""
Pure DataFrame transformations for the Bronze -> Silver -> Gold medallion.

Nothing in this module touches Kafka, S3 or Delta directly: every function takes
a DataFrame and returns a DataFrame. That keeps the business logic unit-testable
with a plain local SparkSession (see tests/test_transformations.py) and lets the
very same functions run on streaming and batch DataFrames.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.schemas import EVENT_SCHEMA, VALID_DEVICE_TYPES, VALID_EVENT_TYPES

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
    ]
)


# Same wire schema plus a column Spark fills with the raw text of unparseable JSON.
CORRUPT_COLUMN = "_corrupt_record"
PARSE_SCHEMA = StructType(EVENT_SCHEMA.fields + [StructField(CORRUPT_COLUMN, StringType(), True)])


def empty_kafka_df(spark: SparkSession) -> DataFrame:
    """Empty DataFrame shaped like the Spark Kafka source (used to derive table schemas)."""
    return spark.createDataFrame([], KAFKA_SCHEMA)


# --------------------------------------------------------------------------- #
# Bronze: raw, immutable copy of what Kafka delivered
# --------------------------------------------------------------------------- #
def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Keep the payload untouched (string) plus Kafka lineage columns."""
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )


# --------------------------------------------------------------------------- #
# Silver: parsed, validated, de-duplicated
# --------------------------------------------------------------------------- #
def _is_blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def _not_in(col: Column, allowed: list) -> Column:
    # isin() yields NULL for NULL input; coalesce so NULL counts as "not allowed".
    return ~F.coalesce(col.isin(allowed), F.lit(False))


def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    """
    Parse the JSON payload and tag every row with `validation_error`
    (NULL when the record is valid). Rules are evaluated in order; the first
    failing rule wins so quarantine rows carry one clear reason.
    """
    parsed = bronze_df.withColumn(
        "_e",
        F.from_json(
            F.col("raw_value"), PARSE_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT_COLUMN}
        ),
    )
    event_ts = F.to_timestamp(F.col("_e.timestamp"))

    error = (
        F.when(
            F.col("_e").isNull() | F.col(f"_e.{CORRUPT_COLUMN}").isNotNull(),
            F.lit("malformed_json"),
        )
        .when(_is_blank(F.col("_e.event_id")), F.lit("missing_event_id"))
        .when(event_ts.isNull(), F.lit("invalid_timestamp"))
        .when(_is_blank(F.col("_e.user_id")), F.lit("missing_user_id"))
        .when(_is_blank(F.col("_e.session_id")), F.lit("missing_session_id"))
        .when(_not_in(F.col("_e.event_type"), VALID_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(_is_blank(F.col("_e.category")), F.lit("missing_category"))
        .when(
            F.col("_e.price").isNull() | (F.col("_e.price") < 0) | F.isnan(F.col("_e.price")),
            F.lit("invalid_price"),
        )
        .when(_not_in(F.col("_e.device_type"), VALID_DEVICE_TYPES), F.lit("invalid_device_type"))
    )

    return parsed.withColumn("_event_ts", event_ts).withColumn("validation_error", error)


def to_silver(parsed_df: DataFrame, late_threshold_seconds: int) -> DataFrame:
    """Typed Silver rows from *valid* parsed records (validation_error IS NULL)."""
    lag_seconds = F.greatest(
        F.lit(0.0),
        F.col("kafka_timestamp").cast("double") - F.col("_event_ts").cast("double"),
    )
    return parsed_df.filter(F.col("validation_error").isNull()).select(
        F.col("_e.event_id").alias("event_id"),
        F.col("_event_ts").alias("event_ts"),
        F.col("_e.user_id").alias("user_id"),
        F.col("_e.session_id").alias("session_id"),
        F.col("_e.event_type").alias("event_type"),
        F.col("_e.product_id").alias("product_id"),
        F.col("_e.category").alias("category"),
        F.round(F.col("_e.price"), 2).alias("price"),
        F.col("_e.device_type").alias("device_type"),
        F.col("_e.country").alias("country"),
        F.col("_e.referrer").alias("referrer"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("ingested_at"),
        lag_seconds.alias("ingest_lag_seconds"),
        (lag_seconds > F.lit(float(late_threshold_seconds))).alias("is_late"),
        F.to_date(F.col("_event_ts")).alias("event_date"),
        F.current_timestamp().alias("silver_processed_at"),
    )


def to_quarantine(parsed_df: DataFrame) -> DataFrame:
    """Dead-letter rows: the untouched payload plus the reason it was rejected."""
    return (
        parsed_df.filter(F.col("validation_error").isNotNull())
        .select(
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_value",
            "validation_error",
            "ingested_at",
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )


def dedupe_batch(df: DataFrame) -> DataFrame:
    """
    Collapse duplicates inside one micro-batch, keeping the earliest Kafka
    position so the choice is deterministic. Cross-batch / replay duplicates are
    removed by the insert-only MERGE into Silver (see delta_io.insert_only_merge).
    """
    w = Window.partitionBy("event_id").orderBy(
        F.col("kafka_partition").asc(), F.col("kafka_offset").asc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# --------------------------------------------------------------------------- #
# Gold: event-time aggregates
# --------------------------------------------------------------------------- #
def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).cast("long")


def aggregate_revenue(silver_df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """
    Tumbling event-time windows per category. Works on batch and streaming
    DataFrames; for streaming, call `.withWatermark("event_ts", ...)` first.
    """
    is_purchase = F.col("event_type") == "purchase"
    agg = silver_df.groupBy(
        F.window(F.col("event_ts"), window_duration).alias("w"), F.col("category")
    ).agg(
        F.count(F.lit(1)).cast("long").alias("events"),
        _count_if(F.col("event_type") == "page_view").alias("page_views"),
        _count_if(F.col("event_type") == "add_to_cart").alias("add_to_carts"),
        _count_if(F.col("event_type") == "checkout_start").alias("checkouts"),
        _count_if(is_purchase).alias("purchases"),
        F.round(F.sum(F.when(is_purchase, F.col("price")).otherwise(F.lit(0.0))), 2).alias("gmv"),
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "category",
        "events",
        "page_views",
        "add_to_carts",
        "checkouts",
        "purchases",
        "gmv",
        F.when(F.col("purchases") > 0, F.round(F.col("gmv") / F.col("purchases"), 2)).alias(
            "avg_order_value"
        ),
        F.to_date(F.col("w.start")).alias("window_date"),
        F.current_timestamp().alias("gold_updated_at"),
    )


def aggregate_funnel(silver_df: DataFrame, window_duration: str = "1 hour") -> DataFrame:
    """Hourly conversion funnel per device (batch-only: uses distinct counts)."""

    def sessions_with(event_type: str) -> Column:
        return F.countDistinct(F.when(F.col("event_type") == event_type, F.col("session_id")))

    agg = silver_df.groupBy(
        F.window(F.col("event_ts"), window_duration).alias("w"), F.col("device_type")
    ).agg(
        F.countDistinct("session_id").cast("long").alias("sessions_total"),
        sessions_with("page_view").cast("long").alias("sessions_browsed"),
        sessions_with("add_to_cart").cast("long").alias("sessions_added_to_cart"),
        sessions_with("checkout_start").cast("long").alias("sessions_checkout_started"),
        sessions_with("purchase").cast("long").alias("sessions_purchased"),
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "device_type",
        "sessions_total",
        "sessions_browsed",
        "sessions_added_to_cart",
        "sessions_checkout_started",
        "sessions_purchased",
        F.when(
            F.col("sessions_total") > 0,
            F.round(F.col("sessions_purchased") / F.col("sessions_total"), 4),
        ).alias("conversion_rate"),
        F.to_date(F.col("w.start")).alias("window_date"),
        F.current_timestamp().alias("gold_updated_at"),
    )


# --------------------------------------------------------------------------- #
# Warehouse hand-off
# --------------------------------------------------------------------------- #
def silver_to_raw_event_json(silver_df: DataFrame) -> DataFrame:
    """
    Serialise Silver rows back to the JSON contract that the dbt staging models
    read from Snowflake (`raw_event:event_id`, `raw_event:timestamp`, ...), so the
    Snowflake RAW table stays identical whether it is fed from Silver or directly
    from Kafka. Returns: event_id, raw_event (JSON string), kafka_partition, kafka_offset.
    """
    payload = F.to_json(
        F.struct(
            F.col("event_id"),
            F.date_format(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("event_type"),
            F.col("product_id"),
            F.col("category"),
            F.col("price"),
            F.col("device_type"),
            F.col("country"),
            F.col("referrer"),
        )
    )
    return silver_df.select(
        "event_id",
        payload.alias("raw_event"),
        "kafka_partition",
        "kafka_offset",
    )
