"""Schemas and constants shared by the streaming jobs, tests and quality checks."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

VALID_EVENT_TYPES = [
    "page_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase",
]
VALID_DEVICE_TYPES = ["mobile", "desktop", "tablet"]

# Wire format produced by data_generator/producer.py (JSON, one event per message).
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("referrer", StringType(), True),
    ]
)

# Column contracts of each medallion table (used by tests and Great Expectations).
SILVER_COLUMNS = [
    "event_id",
    "event_ts",
    "user_id",
    "session_id",
    "event_type",
    "product_id",
    "category",
    "price",
    "device_type",
    "country",
    "referrer",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "ingest_lag_seconds",
    "is_late",
    "event_date",
    "silver_processed_at",
]

GOLD_REVENUE_COLUMNS = [
    "window_start",
    "window_end",
    "category",
    "events",
    "page_views",
    "add_to_carts",
    "checkouts",
    "purchases",
    "gmv",
    "avg_order_value",
    "window_date",
    "gold_updated_at",
]

GOLD_FUNNEL_COLUMNS = [
    "window_start",
    "window_end",
    "device_type",
    "sessions_total",
    "sessions_browsed",
    "sessions_added_to_cart",
    "sessions_checkout_started",
    "sessions_purchased",
    "conversion_rate",
    "window_date",
    "gold_updated_at",
]
