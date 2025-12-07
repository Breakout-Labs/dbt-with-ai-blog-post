with orders as (
    select 
      *
    from {{ ref('stg_ecomm__orders') }}
  ),
  
  transformed as (
    select
      orders.order_id,
      orders.customer_id,
      orders.ordered_at
      orders.order_status,
      orders.total_amount,
    from orders
  ),

  final as (
    select
        *
    from transformed
  )

  select
    *
  from final