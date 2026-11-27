"""
Structured Streaming queries for the three medallion layers.

    Kafka --(bronze)--> Delta bronze --(silver)--> Delta silver --(gold)--> Delta gold

Every hop is its own streaming query with its own checkpoint, so each layer can
be restarted, replayed or backfilled independently.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from lakehouse import delta_io
from lakehouse.config import Settings
from lakehouse.transformations import (
    aggregate_revenue,
    dedupe_batch,
    empty_kafka_df,
    kafka_to_bronze,
    parse_and_validate,
    to_quarantine,
    to_silver,
)

logger = logging.getLogger("lakehouse.streams")

BRONZE_PARTITION = "ingest_date"
SILVER_PARTITION = "event_date"
GOLD_PARTITION = "window_date"


# --------------------------------------------------------------------------- #
# Table bootstrap
# --------------------------------------------------------------------------- #
def bootstrap_tables(spark: SparkSession, s: Settings) -> None:
    """Create all (empty) Delta tables so every stream can start immediately."""
    bronze = kafka_to_bronze(empty_kafka_df(spark))
    parsed = parse_and_validate(bronze)
    silver = to_silver(parsed, s.late_threshold_seconds)
    quarantine = to_quarantine(parsed)
    gold = aggregate_revenue(silver, s.window_duration)

    delta_io.ensure_table(spark, s.bronze_path, bronze, [BRONZE_PARTITION])
    delta_io.ensure_table(spark, s.silver_path, silver, [SILVER_PARTITION])
    delta_io.ensure_table(spark, s.quarantine_path, quarantine)
    delta_io.ensure_table(spark, s.gold_revenue_path, gold, [GOLD_PARTITION])


# --------------------------------------------------------------------------- #
# Bronze: Kafka -> Delta (append-only raw copy)
# --------------------------------------------------------------------------- #
def start_bronze(spark: SparkSession, s: Settings) -> StreamingQuery:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap_servers)
        .option("subscribe", s.kafka_topic)
        .option("startingOffsets", s.starting_offsets)
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        kafka_to_bronze(raw)
        .writeStream.queryName("bronze_kafka_to_delta")
        .format("delta")
        .outputMode("append")
        .partitionBy(BRONZE_PARTITION)
        .option("checkpointLocation", s.checkpoint("bronze"))
        .trigger(processingTime=s.trigger_interval)
        .start(s.bronze_path)
    )


# --------------------------------------------------------------------------- #
# Silver: parse + validate + de-duplicate (idempotent MERGE)
# --------------------------------------------------------------------------- #
def process_bronze_batch(
    spark: SparkSession, batch_df: DataFrame, batch_id: int, s: Settings
) -> None:
    """foreachBatch body for Bronze -> Silver. Safe to replay."""
    parsed = parse_and_validate(batch_df).persist()
    try:
        quarantine = to_quarantine(parsed)
        valid = dedupe_batch(to_silver(parsed, s.late_threshold_seconds))

        if not quarantine.isEmpty():
            delta_io.append_idempotent(quarantine, s.quarantine_path, "silver_quarantine", batch_id)

        if not valid.isEmpty():
            delta_io.insert_only_merge(
                spark, valid, s.silver_path, keys=[SILVER_PARTITION, "event_id"]
            )
    finally:
        parsed.unpersist()


def start_silver(spark: SparkSession, s: Settings) -> StreamingQuery:
    bronze_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", "50")
        .option("skipChangeCommits", "true")
        .load(s.bronze_path)
    )
    return (
        bronze_stream.writeStream.queryName("silver_validate_dedupe")
        .foreachBatch(lambda df, batch_id: process_bronze_batch(spark, df, batch_id, s))
        .option("checkpointLocation", s.checkpoint("silver"))
        .trigger(processingTime=s.trigger_interval)
        .start()
    )


# --------------------------------------------------------------------------- #
# Gold: event-time windows with a watermark
# --------------------------------------------------------------------------- #
def start_gold_revenue(spark: SparkSession, s: Settings) -> StreamingQuery:
    """
    1-minute tumbling windows per category. The watermark bounds state: windows
    older than (max event time - WATERMARK_DELAY) are finalised and records that
    arrive even later are *not* added by the stream. They stay in Silver (flagged
    `is_late`) and are folded back in by the batch reconciliation job
    (lakehouse.jobs.rebuild_gold), which Airflow runs every 15 minutes.
    """
    silver_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", "50")
        .option("skipChangeCommits", "true")
        .load(s.silver_path)
    )
    aggregated = aggregate_revenue(
        silver_stream.withWatermark("event_ts", s.watermark_delay), s.window_duration
    )
    return (
        aggregated.writeStream.queryName("gold_revenue_per_minute")
        .outputMode("update")
        .foreachBatch(
            lambda df, _batch_id: delta_io.upsert(
                spark, df, s.gold_revenue_path, keys=["window_start", "category"]
            )
        )
        .option("checkpointLocation", s.checkpoint("gold_revenue"))
        .trigger(processingTime=s.trigger_interval)
        .start()
    )
