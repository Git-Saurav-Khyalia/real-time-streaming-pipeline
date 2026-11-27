"""Delta Lake write primitives used by the streaming and batch jobs."""

from __future__ import annotations

import logging
import time
from typing import Callable, Iterable, Optional, Sequence, TypeVar

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("lakehouse.delta")

T = TypeVar("T")


def ensure_table(
    spark: SparkSession,
    path: str,
    schema_source: DataFrame,
    partition_by: Optional[Sequence[str]] = None,
) -> None:
    """
    Create an empty Delta table with the right schema if it does not exist yet.
    This lets downstream streams `readStream` a table before the first record
    has been committed to it.
    """
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    writer = schema_source.limit(0).write.format("delta").mode("ignore")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Created Delta table at %s", path)


def _with_retry(fn: Callable[[], T], attempts: int = 4, base_delay: float = 2.0) -> T:
    """Retry on Delta optimistic-concurrency conflicts (streaming + batch writers)."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - py4j wraps the Delta exception types
            retriable = "Concurrent" in type(exc).__name__ or "ConcurrentModification" in str(exc)
            retriable = retriable or "ConcurrentAppendException" in str(exc)
            retriable = retriable or "ConcurrentDeleteReadException" in str(exc)
            if not retriable or attempt == attempts:
                raise
            logger.warning("Delta conflict (attempt %d/%d), retrying: %s", attempt, attempts, exc)
            time.sleep(base_delay * attempt)
    raise RuntimeError("unreachable")  # pragma: no cover


def _condition(keys: Iterable[str]) -> str:
    return " AND ".join(f"t.{k} = s.{k}" for k in keys)


def insert_only_merge(
    spark: SparkSession, df: DataFrame, path: str, keys: Sequence[str]
) -> None:
    """
    MERGE ... WHEN NOT MATCHED THEN INSERT. Idempotent: replaying a micro-batch
    (after a crash, or a Kafka redelivery) never creates duplicate rows. Include
    the partition column in `keys` so Delta only scans the relevant partitions.
    """
    from delta.tables import DeltaTable

    def _run() -> None:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), _condition(keys))
            .whenNotMatchedInsertAll()
            .execute()
        )

    _with_retry(_run)


def upsert(spark: SparkSession, df: DataFrame, path: str, keys: Sequence[str]) -> None:
    """MERGE: update existing rows on key match, insert the rest (Gold aggregates)."""
    from delta.tables import DeltaTable

    def _run() -> None:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), _condition(keys))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    _with_retry(_run)


def append_idempotent(df: DataFrame, path: str, app_id: str, batch_id: int) -> None:
    """
    Append that is skipped when (app_id, batch_id) was already committed, which
    makes foreachBatch writes safe to replay after a failure.
    """
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .save(path)
    )


def compact_and_vacuum(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, path)
    table.optimize().executeCompaction()
    table.vacuum(retain_hours)
