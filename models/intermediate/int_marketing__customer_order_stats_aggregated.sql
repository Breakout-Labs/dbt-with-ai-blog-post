with orders as (
    select 
        * 
    from {{ ref('stg_ecomm__orders') }}
),

aggregated as (
    select
        customer_id,
        -- Aggregate total numbers of order types per each customer
        sum(case when order_status = 'ordered' then 1 else 0 end)
            as orders_ordered,
        sum(case when order_status = 'pending' then 1 else 0 end)
            as orders_pending,
        sum(case when order_status = 'cancelled' then 1 else 0 end)
            as orders_cancelled,
        sum(case when order_status = 'delivered' then 1 else 0 end)
            as orders_delivered,
        count(*) as orders_total,
        round(avg(total_amount), 2) as average_order_amount
    from orders
    group by 1
),

final as (
    select 
        * 
    from aggregated 
)

select * from final