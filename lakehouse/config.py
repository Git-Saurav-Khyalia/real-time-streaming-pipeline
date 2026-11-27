"""
Central configuration for the lakehouse jobs. Everything is driven by
environment variables so the same code runs against:

* local MinIO (S3-compatible, started by docker-compose)  -> default
* real AWS S3                                            -> unset S3_ENDPOINT,
                                                            set LAKEHOUSE_BASE_PATH=s3a://<bucket>
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # --- Kafka ------------------------------------------------------------
    kafka_bootstrap_servers: str
    kafka_topic: str
    starting_offsets: str
    max_offsets_per_trigger: str

    # --- Object storage ---------------------------------------------------
    base_path: str
    s3_endpoint: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str

    # --- Streaming behaviour ----------------------------------------------
    trigger_interval: str
    watermark_delay: str
    window_duration: str
    late_threshold_seconds: int

    # ---- table locations -------------------------------------------------
    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze/ecommerce_events"

    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver/ecommerce_events"

    @property
    def quarantine_path(self) -> str:
        return f"{self.base_path}/silver/quarantine_events"

    @property
    def gold_revenue_path(self) -> str:
        return f"{self.base_path}/gold/revenue_per_minute"

    @property
    def gold_funnel_path(self) -> str:
        return f"{self.base_path}/gold/conversion_funnel_hourly"

    @property
    def validation_results_path(self) -> str:
        return f"{self.base_path}/quality/validation_results"

    def checkpoint(self, name: str) -> str:
        return f"{self.base_path}/_checkpoints/{name}"

    @property
    def bucket(self) -> str:
        """Bucket name parsed from base_path (s3a://bucket/prefix)."""
        if "://" not in self.base_path:
            return ""
        return self.base_path.split("://", 1)[1].split("/", 1)[0]

    @property
    def is_s3(self) -> bool:
        return self.base_path.startswith(("s3a://", "s3://", "s3n://"))


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=_env("KAFKA_TOPIC", "ecommerce_events_raw"),
        starting_offsets=_env("KAFKA_STARTING_OFFSETS", "earliest"),
        max_offsets_per_trigger=_env("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000"),
        base_path=_env("LAKEHOUSE_BASE_PATH", "s3a://ecommerce-lakehouse").rstrip("/"),
        s3_endpoint=_env("S3_ENDPOINT", ""),
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", ""),
        aws_region=_env("AWS_REGION", "us-east-1"),
        trigger_interval=_env("STREAM_TRIGGER_INTERVAL", "10 seconds"),
        watermark_delay=_env("WATERMARK_DELAY", "10 minutes"),
        window_duration=_env("WINDOW_DURATION", "1 minute"),
        late_threshold_seconds=int(_env("LATE_THRESHOLD_SECONDS", "120")),
    )
