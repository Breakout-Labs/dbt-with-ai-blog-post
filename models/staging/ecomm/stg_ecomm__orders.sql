with source as (
    select 
        *
    from {{ source('ecomm', 'orders') }}
),

renamed as (
    select
        id::varchar as order_id,
        customer_id::varchar as customer_id,
        store_id::varchar as store_id,
        status as order_status,
        total_amount,
        created_at as ordered_at,
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