-- stg_transactions.sql
-- Filters the clickstream to completed purchase events and derives
-- transaction-level fields. Built on top of stg_clickstream so
-- de-duplication and typing only happen once, upstream.

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

),

transactions as (

    select
        event_id                                   as transaction_id,
        event_timestamp                             as transaction_timestamp,
        user_id,
        session_id,
        product_id,
        category,
        price                                       as transaction_amount,
        device_type,
        country,
        referrer
    from clickstream
    where event_type = 'purchase'

)

select * from transactions
