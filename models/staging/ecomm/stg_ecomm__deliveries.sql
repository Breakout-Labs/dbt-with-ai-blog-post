with source as (
    select
        *
    from {{ source('ecomm', 'deliveries') }}
  ),

  renamed as (
    select
      id::varchar as delivery_id,
      order_id,
      status as delivery_status,
      picked_up_at,
      delivered_at,
      _synced_at
    from source
  ),

  final as (
    select
      *
    from renamed
  )

  select
    *
  from final