with source as (
  select
    *
  from {{ source('sheets', 'customer_survey_responses') }}
),

transformed as (
  select
    customer_email,
    feedback,
    satisfaction_score,
    {{ parse_date('survey_date') }} as survey_date
  from source
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['customer_email', 'survey_date']) }} as customer_survey_id,
    *
  from transformed
)

select
  *
from final