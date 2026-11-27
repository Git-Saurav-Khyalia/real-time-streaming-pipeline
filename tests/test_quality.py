import json

import pandas as pd
import pytest

from lakehouse import transformations as T
from quality.expectations import SUITES
from quality.runner import validate_by_name
from tests.test_transformations import bronze_df, event


@pytest.fixture()
def silver_pdf(spark):
    events = [
        event(event_id="1", event_type="page_view"),
        event(event_id="2", event_type="add_to_cart", session_id="s2", device_type="desktop"),
        event(event_id="3", event_type="purchase", session_id="s2", device_type="desktop", price=42.5),
    ]
    parsed = T.parse_and_validate(bronze_df(spark, events))
    return T.to_silver(parsed, 120).toPandas()


def test_clean_silver_passes_every_expectation(silver_pdf):
    summary = validate_by_name("silver_events", silver_pdf, SUITES)
    assert summary["failures"] == []
    assert summary["success"] is True
    assert summary["rows_validated"] == 3


def test_duplicate_event_ids_are_caught(silver_pdf):
    dup = pd.concat([silver_pdf, silver_pdf.iloc[[0]]], ignore_index=True)
    summary = validate_by_name("silver_events", dup, SUITES)
    assert summary["success"] is False
    assert any(f["expectation"] == "expect_column_values_to_be_unique" for f in summary["failures"])


def test_schema_drift_is_caught(silver_pdf):
    summary = validate_by_name("silver_events", silver_pdf.drop(columns=["country"]), SUITES)
    assert summary["success"] is False
    assert any(f["expectation"] == "expect_table_columns_to_match_set" for f in summary["failures"])


def test_bad_values_are_caught(silver_pdf):
    bad = silver_pdf.copy()
    bad.loc[0, "price"] = -3.0
    bad.loc[1, "event_type"] = "teleport"
    bad.loc[2, "user_id"] = None
    summary = validate_by_name("silver_events", bad, SUITES)
    failed = {f["expectation"] for f in summary["failures"]}
    assert {
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_be_null",
    } <= failed
    json.dumps(summary)  # summary must be JSON-serialisable (it is stored in Delta)


def test_empty_table_fails_row_count(silver_pdf):
    summary = validate_by_name("silver_events", silver_pdf.iloc[0:0], SUITES)
    assert summary["success"] is False


def test_gold_suites_pass_on_real_aggregates(spark):
    events = [
        event(event_id="1", event_type="purchase", price=10.0),
        event(event_id="2", event_type="page_view", session_id="s2", device_type="tablet"),
    ]
    silver = T.to_silver(T.parse_and_validate(bronze_df(spark, events)), 120)

    revenue = T.aggregate_revenue(silver, "1 minute").toPandas()
    funnel = T.aggregate_funnel(silver, "1 hour").toPandas()

    assert validate_by_name("gold_revenue_per_minute", revenue, SUITES)["success"] is True
    assert validate_by_name("gold_conversion_funnel_hourly", funnel, SUITES)["success"] is True

    broken = revenue.copy()
    broken.loc[0, "gmv"] = -1.0
    assert validate_by_name("gold_revenue_per_minute", broken, SUITES)["success"] is False

    duplicated = pd.concat([revenue, revenue], ignore_index=True)
    assert validate_by_name("gold_revenue_per_minute", duplicated, SUITES)["success"] is False
