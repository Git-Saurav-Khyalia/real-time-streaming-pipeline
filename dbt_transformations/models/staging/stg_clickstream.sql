-- stg_clickstream.sql
-- Parses raw VARIANT event payloads into a strongly-typed row per event,
-- de-duplicated on event_id (Kafka delivery is at-least-once, so the raw
-- table can contain duplicates on redelivery/retry).

with source as (

    select
        raw_event,
        kafka_partition,
        kafka_offset,
        ingested_at
    from {{ source('raw', 'raw_ecommerce_events') }}

),

parsed as (

    select
        raw_event:event_id::string                as event_id,
        raw_event:timestamp::timestamp_ntz         as event_timestamp,
        raw_event:user_id::string                  as user_id,
        raw_event:session_id::string               as session_id,
        raw_event:event_type::string               as event_type,
        raw_event:product_id::string               as product_id,
        raw_event:category::string                 as category,
        raw_event:price::number(10, 2)              as price,
        raw_event:device_type::string               as device_type,
        raw_event:country::string                   as country,
        raw_event:referrer::string                  as referrer,
        kafka_partition,
        kafka_offset,
        ingested_at
    from source

),

deduped as (

    select *,
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as _row_num
    from parsed
    where event_id is not null

)

select
    event_id,
    event_timestamp,
    user_id,
    session_id,
    event_type,
    product_id,
    category,
    price,
    device_type,
    country,
    referrer,
    ingested_at
from deduped
where _row_num = 1
