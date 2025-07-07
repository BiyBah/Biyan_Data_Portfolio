{% test first_date_more_than_second_date(model, first_date_column='order_received_date', second_date_column='pending_delivery_date') %}

with test_date as(
    select 
        {{ first_date_column }}
        , {{ second_date_column }}
    from {{ model }}
    where coalesce({{ first_date_column }}, '9999-12-31'::date) > 
        coalesce({{ second_date_column }}, '9999-12-31'::date)
)
select * from test_date

{% endtest %}