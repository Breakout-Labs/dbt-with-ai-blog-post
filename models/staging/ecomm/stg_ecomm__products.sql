with source as (
    select
        *
    from {{ source('ecomm', 'products') }}
),

renamed as (
    select
        id as product_id,
        name as product_name,
        category as product_category,
        subcategory as product_subcategory,
        unit_price,
        is_active,
        created_at,
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