{% snapshot dim_author %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_author_id",

        strategy="check",
        check_cols=[
            "nk_author_id",
            "author_name"
        ],
        hard_deletes="invalidate"
    )

}}

with 
stg__dim_author as(
    select 
        author_id as nk_author_id
        , author_name
    from {{ source('pacbook_dwh', 'author') }}
),
final_dim_author as(
select 
    {{ dbt_utils.generate_surrogate_key( ["nk_author_id"] ) }} as sk_author_id
    , *
from stg__dim_author
)

select * from final_dim_author

{% endsnapshot %}