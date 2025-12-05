with orders as (
    select
        *
    from {{ ref('orders') }}
), 

customers as (
    select
        *
    from {{ ref('stg_ecomm__customers') }}
),

survey_responses as (
    select
        *
    from {{ ref('stg_sheets__customer_survey_responses') }}
),

customer_metrics as (
    select
        customer_id,
        count(*) as count_orders,
        round(avg(delivery_time_from_collection), 2) as average_delivery_time_from_collection,
        round(avg(delivery_time_from_order), 2) as average_delivery_time_from_order,

        -- Count the number of days between today and the time interval indicates in the list 
        {%- for days in [30,90,360] %}
            count(case when ordered_at > current_date - {{ days }} then 1 end) as count_orders_last_{{ days }}_days
            {%- if not loop.last -%}
                ,
            {%- endif -%}
        {% endfor %},

        min(ordered_at) as first_order_at,
        max(ordered_at) as most_recent_order_at,
    
    from orders
    group by 1

),

joined as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.address,
        customers.phone_number,
        survey_responses.satisfaction_score,
        coalesce(customer_metrics.count_orders,0) as count_orders,
        customer_metrics.average_delivery_time_from_collection,
        customer_metrics.average_delivery_time_from_order,

        -- Select the needed columns for the aggregated order counts
        {%- for days in [30,90,360] %}
            customer_metrics.count_orders_last_{{ days }}_days
            {%- if not loop.last -%}
                ,
            {%- endif -%}
        {% endfor %},

        survey_responses.survey_date,
        customers.created_at,
        customer_metrics.first_order_at,
        customer_metrics.most_recent_order_at,

    from customers
    left join customer_metrics on (
        customers.customer_id = customer_metrics.customer_id
    )
    left join survey_responses on (
        customers.email = survey_responses.customer_email
    )
),

final as (
    select
        *
    from joined
)

select
    *
from final