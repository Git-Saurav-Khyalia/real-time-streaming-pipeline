"""Small helpers around the object store (bucket bootstrap for MinIO)."""

from __future__ import annotations

import logging
import time

from lakehouse.config import Settings

logger = logging.getLogger("lakehouse.storage")


def ensure_bucket(settings: Settings, retries: int = 30, delay_seconds: float = 2.0) -> None:
    """
    Create the lakehouse bucket when running against MinIO (S3_ENDPOINT set).
    Against real AWS S3 we only verify that the bucket is reachable; creating
    buckets is deliberately left to infrastructure-as-code.
    """
    if not settings.is_s3 or not settings.bucket:
        return

    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint or None,
        aws_access_key_id=settings.aws_access_key_id or None,
        aws_secret_access_key=settings.aws_secret_access_key or None,
        region_name=settings.aws_region,
    )

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client.head_bucket(Bucket=settings.bucket)
            logger.info("Bucket '%s' is reachable", settings.bucket)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket", "NotFound") and settings.s3_endpoint:
                client.create_bucket(Bucket=settings.bucket)
                logger.info("Created bucket '%s' on %s", settings.bucket, settings.s3_endpoint)
                return
            last_error = exc
        except Exception as exc:  # endpoint not up yet
            last_error = exc
        logger.info("Waiting for object store (%d/%d): %s", attempt, retries, last_error)
        time.sleep(delay_seconds)

    raise RuntimeError(f"Object store not reachable for bucket '{settings.bucket}': {last_error}")
