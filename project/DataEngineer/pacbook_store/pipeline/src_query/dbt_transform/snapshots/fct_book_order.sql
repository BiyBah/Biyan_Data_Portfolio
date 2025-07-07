{% snapshot fct_book_order %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_line_id",

        strategy="check",
        check_cols="all",
        hard_deletes="invalidate"
    )

}}

with 
stg__dim_cust_order as(
    select 
        order_id as nk_order_id
        , order_date::date as order_date
        , order_date::time as order_time
        , customer_id as nk_customer_id
        , shipping_method_id as nk_shipping_method_id
        , dest_address_id as nk_dest_address_id
    from {{ source('pacbook_dwh', 'cust_order') }}
),
stg__dim_order_history as(
    select 
        order_id as nk_order_id
        , status_id as nk_order_status_id
        , status_date
    from {{ source('pacbook_dwh', 'order_history') }}
),
stg__dim_order_status as(
    select 
        status_id as nk_order_status_id
        , status_value
    from {{ source('pacbook_dwh', 'order_status') }}
),
stg__dim_customer_address as(
    select 
        customer_id as nk_customer_id
        , address_id as nk_dest_address_id
        , status_id as nk_customer_address_status_id
    from {{ source('pacbook_dwh', 'customer_address') }}    
),
stg__dim_address_status as(
    select 
        status_id as nk_customer_address_status_id
        , address_status as dest_address_status
    from {{ source('pacbook_dwh', 'address_status') }}    
),
stg__dim_shipping_method as(
    select 
        method_id as nk_shipping_method_id
        , method_name as shipping_method_name
        , cost as shipping_method_cost
    from {{ source('pacbook_dwh', 'shipping_method') }}    
),
stg__dim_order_line as(
    select 
        line_id as nk_line_id
        , order_id as nk_order_id
        , book_id as nk_book_id
        , price as order_line_price
    from {{ source('pacbook_dwh', 'order_line') }}    
),
int__dim_order_history_status as(
    select
        nk_order_id
        , max(case when 
            sdos.status_value = 'Order Received' 
            then sdoh.status_date::date
            else null end
        ) as order_received_date
        , max(case when 
            sdos.status_value = 'Order Received' 
            then sdoh.status_date::time
            else null end
        ) as order_received_time
        , max(case when 
            sdos.status_value = 'Pending Delivery' 
            then sdoh.status_date::date
            else null end
        ) as pending_delivery_date
        , max(case when 
            sdos.status_value = 'Pending Delivery' 
            then sdoh.status_date::time
            else null end
        ) as pending_delivery_time     
        , max(case when 
            sdos.status_value = 'Delivery In Progress' 
            then sdoh.status_date::date
            else null end
        ) as delivery_in_progress_date
        , max(case when 
            sdos.status_value = 'Delivery In Progress' 
            then sdoh.status_date::time
            else null end
        ) as delivery_in_progress_time
        , max(case when 
            sdos.status_value = 'Delivered' 
            then sdoh.status_date::date
            else null end
        ) as delivered_date
        , max(case when 
            sdos.status_value = 'Delivered' 
            then sdoh.status_date::time
            else null end
        ) as delivered_time
        , max(case when 
            sdos.status_value = 'Cancelled' 
            then sdoh.status_date::date
            else null end
        ) as cancelled_date
        , max(case when 
            sdos.status_value = 'Cancelled' 
            then sdoh.status_date::time
            else null end
        ) as cancelled_time
        , max(case when 
            sdos.status_value = 'Returned' 
            then sdoh.status_date::date
            else null end
        ) as returned_date
        , max(case when 
            sdos.status_value = 'Returned' 
            then sdoh.status_date::time
            else null end
        ) as returned_time
    from stg__dim_order_history sdoh
    left join stg__dim_order_status sdos
        on sdoh.nk_order_status_id = sdos.nk_order_status_id
    group by 1
),
int__customer_address_status as(
    select
        sdca.nk_customer_id
        , sdca.nk_dest_address_id
        , sdca.nk_customer_address_status_id
        , sdas.dest_address_status
    from stg__dim_customer_address sdca
    left join stg__dim_address_status sdas
        on sdca.nk_customer_address_status_id = sdas.nk_customer_address_status_id
),
dim_address as(
    select * from {{ ref("dim_address") }}
),
dim_book as(
    select * from {{ ref("dim_book") }}
),
dim_customer as(
    select * from {{ ref("dim_customer") }}
),
dim_date as(
    select * from {{ ref("dim_date") }}
),
dim_time as(
    select * from {{ ref("dim_time") }}
),
int__fct_book_order as(
    select
        dc.sk_customer_id
        , da.sk_address_id
        , db.sk_book_id
        , dd.sk_date_id
        , dt.time_id
        , sdco.nk_order_id
        , sdco.order_date
        , sdco.order_time
        , sdco.nk_shipping_method_id
        , sdco.nk_dest_address_id
        , idohs.order_received_date
        , idohs.order_received_time
        , idohs.pending_delivery_date
        , idohs.pending_delivery_time
        , idohs.delivery_in_progress_date
        , idohs.delivery_in_progress_time
        , idohs.delivered_date
        , idohs.delivered_time
        , idohs.cancelled_date
        , idohs.cancelled_time
        , idohs.returned_date
        , idohs.returned_time
        , icas.nk_customer_address_status_id
        , icas.dest_address_status
        , sdsm.shipping_method_name
        , sdsm.shipping_method_cost
        , sdol.nk_line_id
        , sdol.nk_book_id
        , sdol.order_line_price
    from stg__dim_cust_order sdco
    left join dim_customer dc
        on sdco.nk_customer_id = dc.nk_customer_id
    left join dim_address da
        on sdco.nk_dest_address_id = da.nk_address_id
    left join stg__dim_order_line sdol
        on sdco.nk_order_id = sdol.nk_order_id
    left join dim_book db
        on sdol.nk_book_id = db.nk_book_id
    left join dim_date dd
        on sdco.order_date = dd.date_day
    left join dim_time dt
        on date_trunc('minute', sdco.order_time) = dt.time_actual::time
    left join int__dim_order_history_status idohs
        on sdco.nk_order_id = idohs.nk_order_id
    left join int__customer_address_status icas
        on sdco.nk_customer_id = icas.nk_customer_id 
        and sdco.nk_dest_address_id = icas.nk_dest_address_id
    left join stg__dim_shipping_method sdsm
        on sdco.nk_shipping_method_id = sdsm.nk_shipping_method_id
),
final_fct_book_order as(
    select 
        {{ dbt_utils.generate_surrogate_key( ["nk_line_id"] ) }} as sk_line_id
        , *
    from int__fct_book_order
)

select * from final_fct_book_order

{% endsnapshot %}