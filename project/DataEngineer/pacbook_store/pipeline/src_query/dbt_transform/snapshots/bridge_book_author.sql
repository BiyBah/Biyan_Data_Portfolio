{% snapshot bridge_book_author %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key=["sk_author_id", "sk_book_id"],

        strategy="check",
        check_cols=[
            "nk_author_id",
            "nk_book_id"
        ],
        hard_deletes="invalidate"
    )

}}

with 
stg__bridge_book_author as(
    select 
        author_id as nk_author_id
        , book_id as nk_book_id
    from {{ source('pacbook_dwh', 'book_author') }}
),
final_bridge_book_author as(
select 
    {{ dbt_utils.generate_surrogate_key( ["nk_author_id"] ) }} as sk_author_id
    , {{ dbt_utils.generate_surrogate_key( ["nk_book_id"] ) }} as sk_book_id
    , *
from stg__bridge_book_author
)

select * from final_bridge_book_author

{% endsnapshot %}