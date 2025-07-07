{% test is_less_than_zero(model, column_name) %}

with test_data as(
    select {{ column_name }}::numeric as column_to_check
    from {{ model }}
),
check_num as(
    select * 
    from test_data
    where column_to_check < 0
)
select * from check_num

{% endtest %}