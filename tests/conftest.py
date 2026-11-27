import os
import sys
import tempfile

import pytest

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def _spark_session():
    """
    One local SparkSession for the whole test run, configured for Delta Lake.
    `delta_ok` records whether the Delta jars could actually be loaded (they are
    downloaded from Maven, so offline machines cannot always do that).
    """
    from pyspark.sql import SparkSession

    def base_builder():
        return (
            SparkSession.builder.master("local[2]")
            .appName("unit-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )

    delta_ok, reason = True, ""
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            base_builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        probe = os.path.join(tempfile.mkdtemp(prefix="delta_probe_"), "t")
        session.range(1).write.format("delta").save(probe)
    except Exception as exc:  # noqa: BLE001  (offline: jars cannot be resolved from Maven)
        delta_ok = False
        reason = (str(exc).splitlines() or [repr(exc)])[0]
        try:
            SparkSession.builder.getOrCreate().stop()
        except Exception:  # noqa: BLE001
            pass
        session = base_builder().getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield session, delta_ok, reason
    session.stop()


@pytest.fixture(scope="session")
def spark(_spark_session):
    """Local Spark for pure-transformation tests."""
    return _spark_session[0]


@pytest.fixture(scope="session")
def delta_spark(_spark_session):
    """Spark with working Delta Lake; skips when the Delta jars are unavailable."""
    session, delta_ok, reason = _spark_session
    if not delta_ok:
        pytest.skip(f"Delta Lake jars unavailable in this environment: {reason}")
    return session
