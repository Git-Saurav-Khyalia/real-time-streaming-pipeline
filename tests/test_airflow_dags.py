"""DAG integrity tests. Skipped when Apache Airflow is not installed (CI installs it)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FOLDER = str(Path(__file__).resolve().parents[1] / "dags")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"ecommerce_lakehouse_pipeline", "lakehouse_maintenance"} <= set(dagbag.dag_ids)


def test_pipeline_task_graph(dagbag):
    dag = dagbag.dags["ecommerce_lakehouse_pipeline"]
    assert {t.task_id for t in dag.tasks} == {
        "validate_silver", "rebuild_gold", "validate_gold", "check_snowflake_configured",
        "load_snowflake_raw", "dbt_deps", "dbt_run", "dbt_test",
    }
    downstream = lambda tid: {t.task_id for t in dag.get_task(tid).downstream_list}  # noqa: E731
    assert downstream("validate_silver") == {"rebuild_gold", "check_snowflake_configured"}
    assert downstream("rebuild_gold") == {"validate_gold"}
    assert downstream("load_snowflake_raw") == {"dbt_deps"}
    assert downstream("dbt_run") == {"dbt_test"}
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_snowflake_branch_is_skipped_without_credentials(monkeypatch):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "pipeline_dag", Path(DAG_FOLDER) / "ecommerce_lakehouse_dag.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for var in module.REQUIRED_SNOWFLAKE_VARS:
        monkeypatch.delenv(var, raising=False)
    assert module.snowflake_is_configured() is False

    for var in module.REQUIRED_SNOWFLAKE_VARS:
        monkeypatch.setenv(var, "x")
    assert module.snowflake_is_configured() is True
