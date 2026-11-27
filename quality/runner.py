"""Run a Great Expectations suite against a pandas DataFrame and summarise the result."""

from __future__ import annotations

import os
from typing import Callable, Dict, List

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx  # noqa: E402
import pandas as pd  # noqa: E402


def validate_dataframe(suite_name: str, df: pd.DataFrame, expectations: List) -> Dict:
    """
    Validate `df` against `expectations` using an in-memory (ephemeral) GX context.
    Returns a JSON-serialisable summary; `success` is True only if every
    expectation passed.
    """
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lakehouse").add_dataframe_asset(suite_name)
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_table")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for expectation in expectations:
        suite.add_expectation(expectation)

    result = batch.validate(suite)

    failures = []
    for item in result.results:
        if not item.success:
            config = item.expectation_config
            failures.append(
                {
                    "expectation": config.type,
                    "column": config.kwargs.get("column") or config.kwargs.get("column_list"),
                    "unexpected_count": item.result.get("unexpected_count"),
                    "observed_value": str(item.result.get("observed_value"))[:200],
                }
            )

    stats = result.statistics
    return {
        "suite": suite_name,
        "success": bool(result.success),
        "rows_validated": int(len(df)),
        "evaluated_expectations": int(stats["evaluated_expectations"]),
        "successful_expectations": int(stats["successful_expectations"]),
        "failures": failures,
    }


def validate_by_name(
    suite_name: str, df: pd.DataFrame, registry: Dict[str, Callable[[], List]]
) -> Dict:
    return validate_dataframe(suite_name, df, registry[suite_name]())
