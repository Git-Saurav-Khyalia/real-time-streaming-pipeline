-- dim_products.sql
-- One row per product: dominant category, price range, views and purchases.

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

)

select
    product_id,
    mode(category)                                         as primary_category,
    min(price)                                             as min_price,
    max(price)                                             as max_price,
    round(avg(price), 2)                                   as avg_price,
    count_if(event_type = 'page_view')                     as view_count,
    count_if(event_type = 'add_to_cart')                   as add_to_cart_count,
    count_if(event_type = 'purchase')                      as purchase_count
from clickstream
where product_id is not null
group by product_id
