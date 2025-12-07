with source as (
    select
        *
    from {{ source('ecomm', 'order_lines') }}
),

renamed as (
    select
        id as order_line_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
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