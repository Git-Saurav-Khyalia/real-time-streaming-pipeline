"""
Great Expectations suites for the medallion tables.

Each function returns a list of expectation objects. They are plain code (no
YAML / checkpoint store to keep in sync), so they are versioned, reviewed and
unit-tested like everything else in the repo.
"""

from __future__ import annotations

import great_expectations.expectations as gxe

from lakehouse.schemas import (
    GOLD_FUNNEL_COLUMNS,
    GOLD_REVENUE_COLUMNS,
    SILVER_COLUMNS,
    VALID_DEVICE_TYPES,
    VALID_EVENT_TYPES,
)


def _not_null(*columns: str) -> list:
    return [gxe.ExpectColumnValuesToNotBeNull(column=c) for c in columns]


def silver_expectations() -> list:
    return [
        # --- schema checks ---------------------------------------------------
        gxe.ExpectTableColumnsToMatchSet(column_set=SILVER_COLUMNS, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        # --- completeness ----------------------------------------------------
        *_not_null(
            "event_id", "event_ts", "user_id", "session_id", "event_type",
            "category", "price", "device_type", "kafka_offset", "silver_processed_at",
        ),
        # --- uniqueness: the de-duplication guarantee of the Silver layer -----
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        # --- validity ----------------------------------------------------------
        gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=VALID_EVENT_TYPES),
        gxe.ExpectColumnValuesToBeInSet(column="device_type", value_set=VALID_DEVICE_TYPES),
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0, max_value=5000),
        gxe.ExpectColumnValuesToBeBetween(column="ingest_lag_seconds", min_value=0, max_value=86400),
        gxe.ExpectColumnValuesToBeInSet(column="is_late", value_set=[True, False]),
    ]


def gold_revenue_expectations() -> list:
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=GOLD_REVENUE_COLUMNS, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        *_not_null("window_start", "window_end", "category", "events", "gmv", "purchases"),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["window_start", "category"]),
        gxe.ExpectColumnValuesToBeBetween(column="gmv", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="purchases", min_value=0),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="events", column_B="purchases", or_equal=True
        ),
    ]


def gold_funnel_expectations() -> list:
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=GOLD_FUNNEL_COLUMNS, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        *_not_null("window_start", "device_type", "sessions_total"),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["window_start", "device_type"]),
        gxe.ExpectColumnValuesToBeBetween(column="conversion_rate", min_value=0, max_value=1),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="sessions_total", column_B="sessions_purchased", or_equal=True
        ),
    ]


SUITES = {
    "silver_events": silver_expectations,
    "gold_revenue_per_minute": gold_revenue_expectations,
    "gold_conversion_funnel_hourly": gold_funnel_expectations,
}
