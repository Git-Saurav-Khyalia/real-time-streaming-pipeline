-- One-time Snowflake bootstrap. Safe to re-run (everything is IF NOT EXISTS).
-- The loader (warehouse/load_silver_to_snowflake.py) also runs the same DDL
-- when the role has the privileges, so this file is mainly for documentation
-- and for locked-down accounts where an admin prepares the objects up front.

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS STREAMING_DB;
CREATE SCHEMA IF NOT EXISTS STREAMING_DB.RAW_SCHEMA;

CREATE TABLE IF NOT EXISTS STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS (
    raw_event          VARIANT,
    kafka_partition    NUMBER,
    kafka_offset       NUMBER,
    ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- dbt creates STREAMING_DB.DBT_DEV_STAGING and STREAMING_DB.DBT_DEV_MARTS itself.
