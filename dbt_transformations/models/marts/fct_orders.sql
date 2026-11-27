-- fct_orders.sql
-- Order-grain fact table (one row per completed purchase) joined to its
-- dimensions by user_id, product_id and session_id.

with transactions as (

    select * from {{ ref('stg_transactions') }}

)

select
    transaction_id                                         as order_id,
    transaction_timestamp                                  as ordered_at,
    date_trunc('day', transaction_timestamp)               as order_date,
    user_id,
    session_id,
    product_id,
    category,
    transaction_amount                                     as order_amount,
    device_type,
    country,
    referrer
from transactions
