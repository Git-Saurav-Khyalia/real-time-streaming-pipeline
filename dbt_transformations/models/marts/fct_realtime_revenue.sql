-- fct_realtime_revenue.sql
-- One row per minute-bucket, with a trailing rolling-window GMV, order count,
-- and cart-abandonment rate. Designed to be queried by Power BI in
-- DirectQuery mode for a live-updating revenue tile.
--
-- rolling_window_minutes is a dbt var (default 5) -> {{ var('rolling_window_minutes') }}

{% set window_minutes = var('rolling_window_minutes') %}

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

),

-- Bucket every event into its minute for a clean, chart-friendly time axis
minute_buckets as (

    select
        date_trunc('minute', event_timestamp)          as minute_bucket,
        event_type,
        price,
        session_id
    from clickstream

),

purchases_per_minute as (

    select
        minute_bucket,
        sum(price)                                       as gmv,
        count(*)                                         as order_count
    from minute_buckets
    where event_type = 'purchase'
    group by 1

),

checkouts_per_minute as (

    select
        minute_bucket,
        count(distinct session_id)                       as checkout_sessions
    from minute_buckets
    where event_type = 'checkout_start'
    group by 1

),

purchase_sessions_per_minute as (

    select
        minute_bucket,
        count(distinct session_id)                       as purchase_sessions
    from minute_buckets
    where event_type = 'purchase'
    group by 1

),

-- Build a gapless minute spine so rolling windows don't skip quiet minutes
spine as (

    select minute_bucket
    from minute_buckets
    group by 1

),

joined as (

    select
        spine.minute_bucket,
        coalesce(p.gmv, 0)                               as gmv,
        coalesce(p.order_count, 0)                       as order_count,
        coalesce(c.checkout_sessions, 0)                 as checkout_sessions,
        coalesce(ps.purchase_sessions, 0)                as purchase_sessions
    from spine
    left join purchases_per_minute p
        on spine.minute_bucket = p.minute_bucket
    left join checkouts_per_minute c
        on spine.minute_bucket = c.minute_bucket
    left join purchase_sessions_per_minute ps
        on spine.minute_bucket = ps.minute_bucket

),

with_rolling as (

    select
        minute_bucket,
        gmv,
        order_count,
        checkout_sessions,
        purchase_sessions,

        -- Rolling GMV / order count over the trailing N-minute window
        sum(gmv) over (
            order by minute_bucket
            range between interval '{{ window_minutes }} minutes' preceding and current row
        )                                                  as rolling_gmv,

        sum(order_count) over (
            order by minute_bucket
            range between interval '{{ window_minutes }} minutes' preceding and current row
        )                                                  as rolling_order_count,

        sum(checkout_sessions) over (
            order by minute_bucket
            range between interval '{{ window_minutes }} minutes' preceding and current row
        )                                                  as rolling_checkout_sessions,

        sum(purchase_sessions) over (
            order by minute_bucket
            range between interval '{{ window_minutes }} minutes' preceding and current row
        )                                                  as rolling_purchase_sessions

    from joined

)

select
    minute_bucket,
    gmv,
    order_count,
    rolling_gmv,
    rolling_order_count,
    round(rolling_gmv / nullif(rolling_order_count, 0), 2)         as rolling_avg_order_value,
    rolling_checkout_sessions,
    rolling_purchase_sessions,
    -- Cart / checkout abandonment rate over the trailing window:
    -- share of checkout-starting sessions that did NOT reach purchase.
    round(
        1 - (rolling_purchase_sessions::float / nullif(rolling_checkout_sessions, 0)),
        4
    )                                                                as rolling_cart_abandonment_rate
from with_rolling
order by minute_bucket
