# Real-Time E-Commerce Streaming Pipeline

An end-to-end streaming analytics platform: a synthetic clickstream generator
publishes live events to **Kafka**, a Python consumer loads them into
**Snowflake**, and **dbt** transforms the raw JSON into analytics-ready
staging and mart tables.

**IMPLEMENTED** (built, run, and tested end-to-end):
```
Python producer → Kafka → Snowflake RAW → dbt staging → dbt marts
```

**OPTIONAL** (not included in this repo — a suggested next step only):
```
Power BI visualization on top of the dbt marts
```
No `.pbix` file, Power BI workspace, or dashboard is included in this
project. Section 3 below is a configuration *guide* for anyone who wants to
build their own Power BI report against the marts — it does not describe
anything shipped here.

```
Faker/Python  ─▶  Kafka (Zookeeper + Schema Registry)  ─▶  Python Consumer
producer.py        ecommerce_events_raw topic              consumer_snowflake.py
                                                                    │
                                                                    ▼
                                              Snowflake STREAMING_DB.RAW_SCHEMA
                                              .RAW_ECOMMERCE_EVENTS (VARIANT)
                                                                    │
                                                                    ▼
                                        dbt: staging (stg_clickstream,
                                        stg_transactions) ─▶ marts
                                        (fct_realtime_revenue,
                                        dim_user_sessions)
                                                                    │
                                                                    ▼
                                        STREAMING_DB.DBT_DEV_STAGING /
                                        STREAMING_DB.DBT_DEV_MARTS
                                        (queryable by any BI tool)
```

---

## 1. Prerequisites

- Docker & Docker Compose
- Python 3.10+
- A Snowflake account (trial account works fine) with a warehouse you can use
- dbt-snowflake (`pip install dbt-snowflake`)
- *(Optional, only if you want a dashboard)* Power BI Desktop or Power BI
  Service, with the Snowflake connector — see Section 3

---

## 2. Local Setup (under 5 minutes)

### Step 1 — Start the Kafka stack

```bash
docker compose up -d
docker compose ps          # confirm zookeeper, kafka, schema-registry, kafka-ui are healthy
```

Kafka UI is available at http://localhost:8080 to visually confirm topics and
messages once the producer is running.

### Step 2 — Set up Snowflake objects

Run once in a Snowflake worksheet (adjust names/warehouse size as needed):

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS STREAMING_DB;
CREATE SCHEMA IF NOT EXISTS STREAMING_DB.RAW_SCHEMA;

CREATE TABLE IF NOT EXISTS STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS (
    raw_event         VARIANT,
    kafka_partition    NUMBER,
    kafka_offset       NUMBER,
    ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

dbt targets this same `STREAMING_DB` database and will auto-create its own
`DBT_DEV_STAGING` and `DBT_DEV_MARTS` schemas on first `dbt run` (no need to
create those schemas by hand). `consumer_snowflake.py` will also auto-create
the raw table above if it doesn't exist, but creating the warehouse/database
up front avoids a first-run race.

### Step 3 — Start the event producer

```bash
cd data_generator
python -m venv venv && source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
python producer.py --rate 5 --topic ecommerce_events_raw --bootstrap-servers localhost:9092
```

You should see periodic `Sent N events (X.X events/sec avg)` log lines.

### Step 4 — Start the Snowflake consumer

In a second terminal:

```bash
cd ingestion
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # fill in your Snowflake credentials
python consumer_snowflake.py --topic ecommerce_events_raw --batch-size 100 --flush-interval 5
```

Confirm rows are landing:

```sql
SELECT COUNT(*) FROM STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS;
```

### Step 5 — Run dbt

```bash
cd dbt_transformations
pip install dbt-snowflake
export DBT_PROFILES_DIR=$(pwd)

export SNOWFLAKE_ACCOUNT=your_account_identifier
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=SYSADMIN
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=STREAMING_DB
export SNOWFLAKE_DBT_SCHEMA=DBT_DEV
export SNOWFLAKE_RAW_DATABASE=STREAMING_DB
export SNOWFLAKE_RAW_SCHEMA=RAW_SCHEMA

dbt deps                 # installs dbt_utils
dbt debug                # verify the connection
dbt source freshness     # checks RAW_ECOMMERCE_EVENTS isn't stale
dbt run
dbt test
```

`SNOWFLAKE_DBT_SCHEMA=DBT_DEV` combines with each model's configured custom
schema (`staging` / `marts` in `dbt_project.yml`) to produce
`DBT_DEV_STAGING` and `DBT_DEV_MARTS` — this is dbt's default schema-naming
behavior, not a separate setting.

For a continuously live dashboard, schedule `dbt source freshness && dbt run`
on a short interval (e.g. every 1–2 minutes via cron, a Snowflake Task, or an
orchestrator such as Airflow/Dagster) so the marts stay fresh against the
streaming raw table.

---

## 3. Power BI Configuration Guide (OPTIONAL — not included in this repo)

This section is a guide for anyone who wants to build their own dashboard on
top of the dbt marts. **No Power BI report, workspace, or `.pbix` file is
part of this project** — nothing here has been built or tested as part of
the pipeline.

### Connecting

1. Power BI Desktop → **Get Data → Snowflake**.
2. Server: `<account>.snowflakecomputing.com`, Warehouse: `COMPUTE_WH`.
3. Choose **DirectQuery** (not Import) so the dashboard reflects new dbt runs
   without manual refresh — critical for a "real-time" feel.
4. Select `STREAMING_DB.DBT_DEV_MARTS.FCT_REALTIME_REVENUE` and
   `STREAMING_DB.DBT_DEV_MARTS.DIM_USER_SESSIONS`.
5. Under **Data source settings**, set an **Automatic Page Refresh** interval
   (Power BI Desktop: report-level "Page refresh" in the Format pane) of
   10–30 seconds so DirectQuery visuals re-poll Snowflake continuously.
6. If publishing to the Power BI Service, configure **Incremental Refresh**
   policies on any Import-mode tables (e.g. a historical archive built on top
   of `fct_realtime_revenue`) partitioned by `minute_bucket`, refreshing only
   the last N days and rewriting the last few hours to catch late-arriving
   corrections.

### Core DAX Measures

```DAX
Real-Time GMV =
CALCULATE(
    MAX(fct_realtime_revenue[rolling_gmv]),
    FILTER(
        fct_realtime_revenue,
        fct_realtime_revenue[minute_bucket] = MAX(fct_realtime_revenue[minute_bucket])
    )
)

Rolling AOV =
CALCULATE(
    MAX(fct_realtime_revenue[rolling_avg_order_value]),
    FILTER(
        fct_realtime_revenue,
        fct_realtime_revenue[minute_bucket] = MAX(fct_realtime_revenue[minute_bucket])
    )
)

Cart Abandonment Rate =
CALCULATE(
    MAX(fct_realtime_revenue[rolling_cart_abandonment_rate]),
    FILTER(
        fct_realtime_revenue,
        fct_realtime_revenue[minute_bucket] = MAX(fct_realtime_revenue[minute_bucket])
    )
)

Active Sessions =
CALCULATE(
    DISTINCTCOUNT(dim_user_sessions[session_id]),
    dim_user_sessions[is_active_session] = TRUE
)

Conversion Rate (Browsed to Purchase) =
DIVIDE(
    CALCULATE(DISTINCTCOUNT(dim_user_sessions[session_id]), dim_user_sessions[funnel_stage] = "purchased"),
    DISTINCTCOUNT(dim_user_sessions[session_id])
)

Sessions by Stage =
CALCULATE(
    DISTINCTCOUNT(dim_user_sessions[session_id])
)   -- used as the base measure inside a funnel visual sliced by [funnel_stage]
```

---

## 4. Dashboard Wireframe — 4-Tile Executive Dashboard (OPTIONAL design guide)

The layout below is a suggested design for the optional Power BI dashboard
from Section 3. It is a wireframe/spec, not a built or included dashboard.

```
┌───────────────────────────────┬───────────────────────────────┐
│ TILE 1: Real-Time GMV Counter  │ TILE 2: Live Conversion Funnel │
│ Card/KPI visual                │ Funnel visual                  │
│  • Measure: [Real-Time GMV]    │  • Axis: dim_user_sessions     │
│  • Trend sparkline beneath,    │    [funnel_stage] ordered      │
│    built on rolling_gmv by     │    (browsed_only → added_to_   │
│    minute_bucket (last 60 min) │    cart → checkout_started →   │
│  • Secondary KPI: [Rolling AOV]│    purchased)                  │
│  • Page refresh: 15s           │  • Values: [Sessions by Stage] │
│                                 │  • Tooltip: drop-off % vs prior│
│                                 │    stage                       │
├───────────────────────────────┼───────────────────────────────┤
│ TILE 3: Top Categories by      │ TILE 4: Traffic by Device      │
│ Active Stream                  │ Donut / 100% stacked bar        │
│ Horizontal bar chart           │  • Legend: device_type          │
│  • Axis: dim_user_sessions     │    (mobile / desktop / tablet)  │
│    [top_category]              │  • Values: DISTINCTCOUNT        │
│  • Values: DISTINCTCOUNT        │    (session_id), filtered to   │
│    (session_id) where           │    is_active_session = TRUE     │
│    is_active_session = TRUE     │  • Drill-through to session    │
│  • Sort descending, top 10      │    list table                   │
└───────────────────────────────┴───────────────────────────────┘
```

**Layout notes**

- Top row = money/funnel (the "so what"), bottom row = composition
  (the "why"/"who").
- Apply a slicer for `country` and a date/time range slicer bound to
  `session_started_at` so the whole page can be scoped to a campaign window.
- Use a consistent color scale: one accent color for GMV/revenue, a second
  for funnel stages, and device-type colors held constant across tiles 2–4
  for visual consistency.
- Set the report-level **Page refresh** (Format pane → Page refresh → On,
  15–30s) so all four DirectQuery visuals re-poll Snowflake automatically —
  this is what makes the dashboard feel "live" without manual interaction.

---

## 5. Data Quality & Freshness

- `models/staging/_sources.yml` defines `freshness.warn_after` /
  `error_after` on `RAW_ECOMMERCE_EVENTS.ingested_at` — run
  `dbt source freshness` on a schedule to alert if the consumer stalls.
- `unique` / `not_null` / `accepted_values` / `dbt_utils.accepted_range`
  tests are attached to every primary key and enum-like column in
  `models/staging/_staging__models.yml` and `models/marts/_marts__models.yml`.
- The staging layer de-duplicates on `event_id`, absorbing Kafka's
  at-least-once redelivery so each event is represented once in the
  staging/marts layer. The Kafka producer itself uses idempotent delivery
  (`enable.idempotence: True`) to avoid duplicate sends on retry — this is
  not an end-to-end exactly-once guarantee for the whole pipeline.

---

## 6. Repository Layout

```
real-time-streaming-pipeline/
├── data_generator/
│   ├── producer.py
│   └── requirements.txt
├── ingestion/
│   ├── consumer_snowflake.py
│   ├── requirements.txt
│   └── .env.example
├── dbt_transformations/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── package-lock.yml
│   └── models/
│       ├── staging/
│       │   ├── _sources.yml
│       │   ├── _staging__models.yml
│       │   ├── stg_clickstream.sql
│       │   └── stg_transactions.sql
│       └── marts/
│           ├── _marts__models.yml
│           ├── fct_realtime_revenue.sql
│           └── dim_user_sessions.sql
├── docker-compose.yml
├── README.md
└── .gitignore
```

Generated artifacts (`dbt_transformations/target/`, `dbt_transformations/dbt_packages/`,
`dbt_transformations/logs/`, `__pycache__/`, `*.pyc`) are excluded from this
repo via `.gitignore` and recreated automatically by `dbt deps` / `dbt run` /
running the Python scripts.

## 7. Shutting Down

```bash
# stop the Python producer/consumer with Ctrl+C (both handle SIGINT/SIGTERM
# gracefully and flush in-flight batches before exiting)
docker compose down -v     # stop and remove Kafka/Zookeeper/Schema Registry + volumes
```
