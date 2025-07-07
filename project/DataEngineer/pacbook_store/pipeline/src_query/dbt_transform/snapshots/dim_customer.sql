{% snapshot dim_customer %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_customer_id",

        strategy="check",
        check_cols=[
            "nk_customer_id",
            "first_name",
            "last_name",
            "email"
        ],
        hard_deletes="invalidate"
    )

}}

with 
stg__dim_customer as(
    select 
        customer_id as nk_customer_id
        , first_name
        , last_name
        , email
    from {{ source('pacbook_dwh', 'customer') }}
),
final_dim_customer as(
select 
    {{ dbt_utils.generate_surrogate_key( ["nk_customer_id"] ) }} as sk_customer_id
    , *
from stg__dim_customer
)

select * from final_dim_customer

{% endsnapshot %}