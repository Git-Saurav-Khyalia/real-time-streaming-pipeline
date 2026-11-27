# Power BI guide (optional, not built)

Suggested configuration for a dashboard on the dbt marts. No `.pbix` or workspace is included in this repo.

## Connecting

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

## Dashboard wireframe — 4-Tile Executive Dashboard (OPTIONAL design guide)

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

