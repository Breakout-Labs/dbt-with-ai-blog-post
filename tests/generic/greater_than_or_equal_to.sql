{% test greater_than_or_equal_to(model, column_name, value=0) %}

with validation as (
    select
        {{ column_name }} as test_field
    from {{ model }}
),

validation_errors as (
    select
        test_field
    from validation
    /*
    Q: What is this, test_field is smaller than value? We're supposed to test for greater than or equal (>=).
    Can you explain?
    */
    where test_field < {{ value }}  

)

select *
from validation_errors

{% endtest %}