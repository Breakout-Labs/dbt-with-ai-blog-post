with orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

order_lines as (
    select
        *
    from {{ ref('stg_ecomm__order_lines') }}
),

products as (
    select
        *
    from {{ ref('stg_ecomm__products') }}
),

orders_with_lines as (
    select
        orders.order_id,
        orders.order_status,
        orders.ordered_at,
        order_lines.order_line_id,
        order_lines.product_id,
        order_lines.quantity,
        order_lines.unit_price,
        order_lines.line_total
    from orders
    inner join order_lines
        on orders.order_id = order_lines.order_id
),

orders_with_products as (
    select
        orders_with_lines.*,
        products.product_name,
        products.product_category,
        products.product_subcategory
    from orders_with_lines
    inner join products
        on orders_with_lines.product_id = products.product_id
),

aggregated as (
    select
        to_varchar(date_trunc('month', ordered_at), 'YYYY-MM') as order_month,
        product_category,
        product_subcategory,
        count(distinct order_id) as order_count,
        sum(quantity) as total_quantity_sold,
        round(sum(line_total), 2) as total_revenue,
        round(avg(unit_price), 2) as average_unit_price
    from orders_with_products
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_category', 'product_subcategory', 'order_month']) }} as product_subcategory_month_id,
        *
    from aggregated
    order by order_month
)

select
    *
from final