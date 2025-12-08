with deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries_with_customer as (
    select
        deliveries.delivery_id,
        orders.customer_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders
        on deliveries.order_id = orders.order_id
),

aggregated as (
    select
        customer_id,
        count(*) as total_delivery_count,
        count(case when delivery_status = 'delivered' then 1 end) as delivered_delivery_count,
        count(case when delivery_status = 'picked_up' then 1 end) as picked_up_delivery_count,
        count(case when delivery_status = 'cancelled' then 1 end) as cancelled_delivery_count,
        max(delivered_at) as last_delivery_timestamp
    from deliveries_with_customer
    group by customer_id
),

final as (
    select
        *
    from aggregated
)

select
    *
from final
