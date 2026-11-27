-- dim_users.sql
-- One row per user: first/last activity, session and order counts, lifetime value.

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

)

select
    user_id,
    min(event_timestamp)                                   as first_seen_at,
    max(event_timestamp)                                   as last_seen_at,
    count(distinct session_id)                             as total_sessions,
    count(*)                                               as total_events,
    count_if(event_type = 'purchase')                      as total_orders,
    sum(case when event_type = 'purchase' then price else 0 end) as lifetime_value,
    mode(device_type)                                      as favorite_device,
    mode(country)                                          as primary_country
from clickstream
where user_id is not null
group by user_id
