with customers as (
    select
        *
    from {{ ref('customers') }}
),

customer_order_stats_aggregated as (
    select
        *
    from {{ ref('int_marketing__customer_order_stats_aggregated') }}
),

joined as (
    select
        customer_id,
        customers.first_name,
        customers.last_name,
        customer_order_stats_aggregated.* exclude (customer_id)
    from customers
    inner join customer_order_stats_aggregated using (customer_id)
)

select
    *
from joined