with source as (
  select
    *
  from {{ source('ecomm', 'customers') }}
),

renamed as (
  select
    id::varchar as customer_id,
    * exclude id
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