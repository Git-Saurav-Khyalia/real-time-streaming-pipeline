-- dim_user_sessions.sql
-- One row per session_id, summarizing funnel progress, device, timing, and
-- monetary outcome. Powers "active sessions", "top categories", and
-- "traffic by device dashboard tiles."

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

),

session_events as (

    select
        session_id,

        any_value(user_id)                                      as user_id,
        any_value(device_type)                                 as device_type,
        any_value(country)                                     as country,
        any_value(referrer)                                    as referrer,

        min(event_timestamp)                                   as session_started_at,
        max(event_timestamp)                                   as session_last_event_at,

        count(*)                                               as total_events,

        count_if(event_type = 'page_view')                     as page_views,
        count_if(event_type = 'add_to_cart')                   as add_to_cart_events,
        count_if(event_type = 'remove_from_cart')              as remove_from_cart_events,
        count_if(event_type = 'checkout_start')                as checkout_start_events,
        count_if(event_type = 'purchase')                      as purchase_events,

        sum(
            case
                when event_type = 'purchase' then price
                else 0
            end
        )                                                       as session_revenue,

        mode(category)                                          as top_category

    from clickstream

    where session_id is not null

    group by session_id

),

funnel_stage as (

    select
        *,

        case
            when purchase_events > 0
                then 'purchased'

            when checkout_start_events > 0
                then 'checkout_started'

            when add_to_cart_events > 0
                then 'added_to_cart'

            when page_views > 0
                then 'browsed_only'

            else 'unknown'
        end                                                     as funnel_stage,

        datediff(
            'second',
            session_started_at,
            session_last_event_at
        )                                                       as session_duration_seconds,

        case
            when session_last_event_at >= dateadd(
                'minute',
                -10,
                (select max(event_timestamp) from clickstream)
            )
            then true
            else false
        end                                                     as is_active_session

    from session_events

)

select
    session_id,
    user_id,
    device_type,
    country,
    referrer,
    top_category,
    funnel_stage,
    is_active_session,
    session_started_at,
    session_last_event_at,
    session_duration_seconds,
    total_events,
    page_views,
    add_to_cart_events,
    remove_from_cart_events,
    checkout_start_events,
    purchase_events,
    session_revenue

from funnel_stage