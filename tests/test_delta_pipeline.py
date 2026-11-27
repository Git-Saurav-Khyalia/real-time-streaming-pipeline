"""
Delta Lake integration tests (MERGE idempotency, replay safety, Gold upserts).

They need the Delta jars from Maven, so they skip themselves when those cannot be
resolved (offline sandboxes) and run for real in GitHub Actions.
"""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from lakehouse import streams
from lakehouse import transformations as T
from lakehouse.config import load_settings
from lakehouse.jobs.rebuild_gold import rebuild

pytestmark = pytest.mark.delta


def make_event(event_id, *, minutes_ago=1, event_type="purchase", price=10.0, **kw):
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    e = {
        "event_id": event_id,
        "timestamp": ts.isoformat(),
        "user_id": "user_1",
        "session_id": "s1",
        "event_type": event_type,
        "product_id": "prod_1",
        "category": "books",
        "price": price,
        "device_type": "mobile",
        "country": "IN",
        "referrer": "direct",
    }
    e.update(kw)
    return e


@pytest.fixture()
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BASE_PATH", str(tmp_path / "lake"))
    return load_settings()


def bronze_batch(spark, payloads, start_offset=0):
    import json

    now = datetime.now(timezone.utc)
    rows = [
        (None, p if isinstance(p, str) else json.dumps(p), "t", 0, start_offset + i, now)
        for i, p in enumerate(payloads)
    ]
    kafka = spark.createDataFrame(
        rows, "key binary, value string, topic string, partition int, offset long, timestamp timestamp"
    ).withColumn("value", F.col("value").cast("binary"))
    return T.kafka_to_bronze(kafka)


def test_silver_batch_is_idempotent_and_deduplicated(delta_spark, settings):
    spark = delta_spark
    streams.bootstrap_tables(spark, settings)

    payloads = [
        make_event("e1"),
        make_event("e2", event_type="page_view"),
        make_event("e1"),  # duplicate inside the same batch
        "{broken json",    # goes to quarantine
        make_event("e3", price=-1),  # invalid price -> quarantine
    ]
    batch = bronze_batch(spark, payloads)

    streams.process_bronze_batch(spark, batch, 0, settings)
    streams.process_bronze_batch(spark, batch, 0, settings)  # replay of the same micro-batch

    silver = spark.read.format("delta").load(settings.silver_path)
    assert sorted(r.event_id for r in silver.collect()) == ["e1", "e2"]

    quarantine = spark.read.format("delta").load(settings.quarantine_path)
    assert quarantine.count() == 2  # replay did not double-write (txnAppId/txnVersion)

    # A *later* batch re-delivering e1 (Kafka redelivery) is also ignored.
    streams.process_bronze_batch(spark, bronze_batch(spark, [make_event("e1")], 100), 1, settings)
    assert spark.read.format("delta").load(settings.silver_path).count() == 2


def test_late_events_are_kept_in_silver_and_flagged(delta_spark, settings):
    spark = delta_spark
    streams.bootstrap_tables(spark, settings)
    very_late = make_event("late1", minutes_ago=90)
    streams.process_bronze_batch(spark, bronze_batch(spark, [very_late]), 0, settings)

    row = spark.read.format("delta").load(settings.silver_path).first()
    assert row.event_id == "late1" and row.is_late is True


def test_rebuild_gold_upserts_and_is_repeatable(delta_spark, settings):
    spark = delta_spark
    streams.bootstrap_tables(spark, settings)
    payloads = [
        make_event("p1", minutes_ago=3, price=20.0),
        make_event("p2", minutes_ago=3, price=30.0),
        make_event("v1", minutes_ago=3, event_type="page_view"),
    ]
    streams.process_bronze_batch(spark, bronze_batch(spark, payloads), 0, settings)

    first = rebuild(spark, settings, lookback_hours=2)
    assert first["revenue_windows_upserted"] >= 1
    second = rebuild(spark, settings, lookback_hours=2)  # re-running must not duplicate rows
    assert second == first

    gold = spark.read.format("delta").load(settings.gold_revenue_path)
    assert gold.groupBy("window_start", "category").count().filter("count > 1").count() == 0
    assert gold.agg(F.sum("gmv")).first()[0] == pytest.approx(50.0)

    funnel = spark.read.format("delta").load(settings.gold_funnel_path)
    assert funnel.agg(F.sum("sessions_purchased")).first()[0] >= 1


def test_rebuild_corrects_gold_after_a_late_record(delta_spark, settings):
    """A record that missed the streaming watermark is folded in by the batch rebuild."""
    spark = delta_spark
    streams.bootstrap_tables(spark, settings)
    streams.process_bronze_batch(
        spark, bronze_batch(spark, [make_event("a", minutes_ago=5, price=10.0)]), 0, settings
    )
    rebuild(spark, settings, 2)
    before = spark.read.format("delta").load(settings.gold_revenue_path).agg(F.sum("gmv")).first()[0]

    streams.process_bronze_batch(
        spark, bronze_batch(spark, [make_event("b", minutes_ago=5, price=15.0)], 10), 1, settings
    )
    rebuild(spark, settings, 2)
    after = spark.read.format("delta").load(settings.gold_revenue_path).agg(F.sum("gmv")).first()[0]
    assert before == pytest.approx(10.0)
    assert after == pytest.approx(25.0)
