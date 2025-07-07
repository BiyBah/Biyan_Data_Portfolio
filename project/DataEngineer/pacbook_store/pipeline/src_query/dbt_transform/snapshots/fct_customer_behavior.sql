{% snapshot fct_customer_behavior %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_customer_id",

        strategy="check",
        check_cols="all",
        hard_deletes="invalidate"
    )

}}

with 
stg__dim_cust_order as(
    select
        order_id as nk_order_id
        , order_date
        , customer_id as nk_customer_id
        , shipping_method_id as nk_shipping_method_id
        , dest_address_id as nk_dest_address_id
    from {{ source('pacbook_dwh', 'cust_order') }}
),
stg__dim_order_line as(
    select 
        line_id as nk_line_id
        , order_id as nk_order_id
        , book_id as nk_book_id
        , price as order_line_price
    from {{ source('pacbook_dwh', 'order_line') }}    
),
dim_customer as(
    select * from {{ ref("dim_customer") }}
),
dim_date as(
    select * from {{ ref("dim_date") }}
),
int__prev_order_date as(
    select
        nk_customer_id
        , order_date::date
        , lag(order_date, 1, null) over(partition by nk_customer_id order by order_date)::date as previous_order_date
    from stg__dim_cust_order
),
int__time_between_order as(
    select 
        nk_customer_id
        , order_date
        , previous_order_date
        , extract(epoch from age(order_date, previous_order_date)) / (60 * 60 * 24) as days_between_orders
        , extract(month from age(order_date, previous_order_date)) + extract(year from age(order_date, previous_order_date)) * 12 as months_between_orders
        , extract(year from age(order_date, previous_order_date)) as years_between_orders
    from int__prev_order_date
),
int__avg_time_between_order as(
    select
        nk_customer_id
        , coalesce(avg(days_between_orders),0) as avg_days_between_orders
        , coalesce(avg(months_between_orders),0) as avg_months_between_orders
        , coalesce(avg(years_between_orders),0) as avg_years_between_orders
        , max(order_date) as last_order_date
    from int__time_between_order
    group by 1
),
int__sales_amt_per_order as(
    select
        nk_order_id
        , sum(order_line_price) as sales_amt_per_order
    from stg__dim_order_line
    group by 1
),
int__avg_order_value_per_cust as(
    select
        sdco.nk_customer_id
        , avg(isapo.sales_amt_per_order) as average_order_value
        , count(distinct isapo.nk_order_id) as order_frequency
    from stg__dim_cust_order sdco
    left join int__sales_amt_per_order isapo
        on sdco.nk_order_id = isapo.nk_order_id
    group by 1
),
int__fct_customer_behavior as(
    select
        dd.sk_date_id
        , iatbo.nk_customer_id
        , iatbo.avg_days_between_orders
        , iatbo.avg_months_between_orders
        , iatbo.avg_years_between_orders
        , iatbo.last_order_date::date
        , iaovpc.order_frequency
        , iaovpc.average_order_value
    from int__avg_time_between_order iatbo
    left join int__avg_order_value_per_cust iaovpc
        on iatbo.nk_customer_id = iaovpc.nk_customer_id
    left join dim_date dd
        on iatbo.last_order_date = dd.date_day
),
final_dim_customer_behavior as(
    select 
        {{ dbt_utils.generate_surrogate_key( ["nk_customer_id"] ) }} as sk_customer_id
        , *
    from int__fct_customer_behavior
)

select * from final_dim_customer_behavior

{% endsnapshot %}