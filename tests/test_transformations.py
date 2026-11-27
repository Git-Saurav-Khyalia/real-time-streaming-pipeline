from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from lakehouse import transformations as T
from lakehouse.schemas import (
    GOLD_FUNNEL_COLUMNS,
    GOLD_REVENUE_COLUMNS,
    SILVER_COLUMNS,
)

NOW = datetime(2026, 11, 25, 10, 0, 30, tzinfo=timezone.utc)


def event(**overrides):
    base = {
        "event_id": "e1",
        "timestamp": NOW.isoformat(),
        "user_id": "user_a",
        "session_id": "s1",
        "event_type": "page_view",
        "product_id": "prod_1",
        "category": "books",
        "price": 10.0,
        "device_type": "mobile",
        "country": "IN",
        "referrer": "google",
    }
    base.update(overrides)
    return base


def bronze_df(spark, payloads, kafka_ts=NOW, start_offset=0):
    """Bronze-shaped DataFrame from python dicts / raw strings."""
    import json

    rows = []
    for i, p in enumerate(payloads):
        value = p if isinstance(p, str) or p is None else json.dumps(p)
        rows.append((None, value, "ecommerce_events_raw", 0, start_offset + i, kafka_ts))
    kafka = spark.createDataFrame(
        rows,
        "key binary, value string, topic string, partition int, offset long, timestamp timestamp",
    ).withColumn("value", F.col("value").cast("binary"))
    return T.kafka_to_bronze(kafka)


# ------------------------------------------------------------------ bronze
def test_kafka_to_bronze_keeps_payload_and_lineage(spark):
    df = bronze_df(spark, [event()])
    row = df.first()
    assert row["raw_value"].startswith("{")
    assert row["kafka_partition"] == 0 and row["kafka_offset"] == 0
    assert row["kafka_topic"] == "ecommerce_events_raw"
    assert row["ingest_date"] is not None


# ------------------------------------------------------------------ silver
@pytest.mark.parametrize(
    "payload,expected_error",
    [
        (event(), None),
        ("{not json", "malformed_json"),
        (None, "malformed_json"),
        (event(event_id=None), "missing_event_id"),
        (event(timestamp="yesterday-ish"), "invalid_timestamp"),
        (event(user_id=""), "missing_user_id"),
        (event(session_id=None), "missing_session_id"),
        (event(event_type="teleport"), "invalid_event_type"),
        (event(category=None), "missing_category"),
        (event(price=-5), "invalid_price"),
        (event(price=None), "invalid_price"),
        (event(device_type="fridge"), "invalid_device_type"),
    ],
)
def test_validation_rules(spark, payload, expected_error):
    parsed = T.parse_and_validate(bronze_df(spark, [payload]))
    assert parsed.first()["validation_error"] == expected_error


def test_to_silver_schema_types_and_lateness(spark):
    on_time = event(event_id="on_time", timestamp=(NOW - timedelta(seconds=5)).isoformat())
    late = event(event_id="late", timestamp=(NOW - timedelta(minutes=30)).isoformat())
    parsed = T.parse_and_validate(bronze_df(spark, [on_time, late]))

    silver = T.to_silver(parsed, late_threshold_seconds=120)
    assert silver.columns == SILVER_COLUMNS

    rows = {r["event_id"]: r for r in silver.collect()}
    assert rows["on_time"]["is_late"] is False
    assert rows["late"]["is_late"] is True
    assert rows["late"]["ingest_lag_seconds"] == pytest.approx(1800.0)
    assert rows["on_time"]["event_ts"] == (NOW - timedelta(seconds=5)).replace(tzinfo=None)
    assert str(rows["on_time"]["event_date"]) == "2026-11-25"


def test_quarantine_keeps_raw_payload_and_reason(spark):
    parsed = T.parse_and_validate(bronze_df(spark, [event(), "{broken", event(price=-1, event_id="x")]))
    q = {r["raw_value"][:7]: r["validation_error"] for r in T.to_quarantine(parsed).collect()}
    assert sorted(q.values()) == ["invalid_price", "malformed_json"]
    assert T.to_silver(parsed, 120).count() == 1


def test_dedupe_batch_keeps_earliest_offset(spark):
    dup_a = event(event_id="dup")
    dup_b = event(event_id="dup")
    other = event(event_id="other")
    parsed = T.parse_and_validate(bronze_df(spark, [dup_a, other, dup_b], start_offset=10))
    silver = T.dedupe_batch(T.to_silver(parsed, 120))
    got = {r["event_id"]: r["kafka_offset"] for r in silver.collect()}
    assert got == {"dup": 10, "other": 11}


# -------------------------------------------------------------------- gold
def silver_rows(spark, events):
    parsed = T.parse_and_validate(bronze_df(spark, events))
    return T.to_silver(parsed, 120)


def test_aggregate_revenue_windows_and_metrics(spark):
    t0 = datetime(2026, 11, 25, 10, 0, 10, tzinfo=timezone.utc)
    events = [
        event(event_id="1", timestamp=t0.isoformat(), event_type="page_view"),
        event(event_id="2", timestamp=(t0 + timedelta(seconds=5)).isoformat(), event_type="add_to_cart"),
        event(event_id="3", timestamp=(t0 + timedelta(seconds=10)).isoformat(), event_type="purchase", price=30.0),
        event(event_id="4", timestamp=(t0 + timedelta(seconds=15)).isoformat(), event_type="purchase", price=10.0),
        # next minute window
        event(event_id="5", timestamp=(t0 + timedelta(seconds=70)).isoformat(), event_type="purchase", price=5.5),
        # other category, same minute
        event(event_id="6", timestamp=t0.isoformat(), event_type="checkout_start", category="grocery"),
    ]
    gold = T.aggregate_revenue(silver_rows(spark, events), "1 minute")
    assert gold.columns == GOLD_REVENUE_COLUMNS

    rows = {(str(r["window_start"]), r["category"]): r for r in gold.collect()}
    first = rows[("2026-11-25 10:00:00", "books")]
    assert (first["events"], first["page_views"], first["add_to_carts"], first["purchases"]) == (4, 1, 1, 2)
    assert first["gmv"] == pytest.approx(40.0)
    assert first["avg_order_value"] == pytest.approx(20.0)

    second = rows[("2026-11-25 10:01:00", "books")]
    assert second["gmv"] == pytest.approx(5.5) and second["purchases"] == 1

    grocery = rows[("2026-11-25 10:00:00", "grocery")]
    assert grocery["checkouts"] == 1 and grocery["gmv"] == 0.0 and grocery["avg_order_value"] is None


def test_aggregate_funnel_counts_distinct_sessions(spark):
    ts = NOW.isoformat()
    events = [
        event(event_id="1", session_id="s1", event_type="page_view", timestamp=ts),
        event(event_id="2", session_id="s1", event_type="page_view", timestamp=ts),  # same session twice
        event(event_id="3", session_id="s1", event_type="add_to_cart", timestamp=ts),
        event(event_id="4", session_id="s1", event_type="purchase", timestamp=ts),
        event(event_id="5", session_id="s2", event_type="page_view", timestamp=ts),
    ]
    funnel = T.aggregate_funnel(silver_rows(spark, events), "1 hour")
    assert funnel.columns == GOLD_FUNNEL_COLUMNS
    row = funnel.first()
    assert row["sessions_total"] == 2
    assert row["sessions_browsed"] == 2
    assert row["sessions_added_to_cart"] == 1
    assert row["sessions_purchased"] == 1
    assert row["conversion_rate"] == pytest.approx(0.5)


# --------------------------------------------------- streaming = batch result
def test_streaming_revenue_matches_batch_with_watermark(spark, tmp_path):
    """
    Same aggregate function on a *streaming* DataFrame (file source, update
    mode, watermark). A record older than the watermark must not change a
    finalised window, which is exactly what the batch reconciliation job fixes.
    """
    src = tmp_path / "silver_src"
    t0 = datetime(2026, 11, 25, 10, 0, 5, tzinfo=timezone.utc)
    events = [
        event(event_id="a", timestamp=t0.isoformat(), event_type="purchase", price=10.0),
        event(event_id="b", timestamp=(t0 + timedelta(seconds=20)).isoformat(), event_type="purchase", price=20.0),
        # advances event time by 30 minutes -> watermark (10 min) passes the 10:00 window
        event(event_id="c", timestamp=(t0 + timedelta(minutes=30)).isoformat(), event_type="purchase", price=1.0),
    ]
    silver_rows(spark, events).write.mode("overwrite").parquet(str(src))

    stream = (
        spark.readStream.schema(spark.read.parquet(str(src)).schema)
        .parquet(str(src))
        .withWatermark("event_ts", "10 minutes")
    )
    agg = T.aggregate_revenue(stream, "1 minute")

    collected = []

    def sink(df, _batch_id):
        collected.extend(df.collect())

    query = (
        agg.writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(120)

    by_window = {str(r["window_start"]): r["gmv"] for r in collected}
    assert by_window["2026-11-25 10:00:00"] == pytest.approx(30.0)
    assert by_window["2026-11-25 10:30:00"] == pytest.approx(1.0)


# --------------------------------------------------------------- warehouse
def test_silver_to_raw_event_json_matches_dbt_contract(spark):
    import json

    silver = silver_rows(spark, [event(event_id="abc", price=12.345)])
    row = T.silver_to_raw_event_json(silver).first()
    payload = json.loads(row["raw_event"])
    assert set(payload) == {
        "event_id", "timestamp", "user_id", "session_id", "event_type",
        "product_id", "category", "price", "device_type", "country", "referrer",
    }
    assert payload["timestamp"] == "2026-11-25T10:00:30.000Z"
    assert payload["price"] == pytest.approx(12.35, abs=0.01)
    assert row["event_id"] == "abc"


# ------------------------------------------------- table bootstrap schemas
def test_table_schemas_can_be_derived_from_an_empty_kafka_frame(spark):
    """bootstrap_tables() derives every Delta schema this way, so it must work on 0 rows."""
    bronze = T.kafka_to_bronze(T.empty_kafka_df(spark))
    parsed = T.parse_and_validate(bronze)
    silver = T.to_silver(parsed, 120)
    gold = T.aggregate_revenue(silver, "1 minute")
    funnel = T.aggregate_funnel(silver, "1 hour")

    assert bronze.count() == 0
    assert silver.columns == SILVER_COLUMNS
    assert gold.columns == GOLD_REVENUE_COLUMNS
    assert funnel.columns == GOLD_FUNNEL_COLUMNS
    assert set(T.to_quarantine(parsed).columns) == {
        "kafka_partition", "kafka_offset", "kafka_timestamp", "raw_value",
        "validation_error", "ingested_at", "quarantined_at",
    }
