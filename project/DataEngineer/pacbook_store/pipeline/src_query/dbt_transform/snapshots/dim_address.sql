{% snapshot dim_address %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_address_id",

        strategy="check",
        check_cols=[
            "nk_address_id",
            "street_name",
            "city",
            "nk_country_id",
            "country_name"
        ],
        hard_deletes="invalidate"
    )
}}

with 
stg__dim_address as(
    select 
        address_id as nk_address_id
        , street_name
        , city
        , country_id as nk_country_id
    from {{ source('pacbook_dwh', 'address') }}
),
stg__dim_country as(
    select 
        country_id as nk_country_id
        , country_name
    from {{ source('pacbook_dwh', 'country') }}
),
dim_address as(
    select 
        sda.nk_address_id
        , sda.street_name
        , sda.city
        , sdc.nk_country_id
        , sdc.country_name
    from stg__dim_address sda
    left join stg__dim_country sdc
        on sda.nk_country_id = sdc.nk_country_id
),
final_dim_address as(
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_address_id"] ) }} as sk_address_id
        , *
    from dim_address
)

select * from final_dim_address

{% endsnapshot %}