{% snapshot dim_book %}

{{
    config(
        target_database="pacbook_dwh",
        target_schema="mart",
        unique_key="sk_book_id",

        strategy="check",
        check_cols=[
            "nk_book_id",
            "title",
            "isbn13",
            "num_pages",
            "publication_date",
            "nk_publisher_id",
            "publisher_name",
            "nk_language_id",
            "language_code",
            "language_name"
        ],
        hard_deletes="invalidate"
    )

}}

with 
stg__dim_book as(
    select 
        book_id as nk_book_id
        , title
        , isbn13
        , language_id as nk_language_id
        , num_pages
        , publication_date
        , publisher_id as nk_publisher_id
    from {{ source('pacbook_dwh', 'book') }}
),
stg__dim_publisher as(
    select 
        publisher_id as nk_publisher_id
        , publisher_name
    from {{ source('pacbook_dwh', 'publisher') }}
),
stg__dim_book_language as(
    select 
        language_id as nk_language_id
        , language_code
        , language_name
    from {{ source('pacbook_dwh', 'book_language') }}
),
dim_book as(
    select
        sdb.nk_book_id
        , sdb.title
        , sdb.isbn13
        , sdb.nk_language_id
        , sdb.num_pages
        , sdb.publication_date
        , sdb.nk_publisher_id
        , sdp.publisher_name
        , sdbl.language_code
        , sdbl.language_name
    from stg__dim_book sdb
    left join stg__dim_publisher sdp
        on sdb.nk_publisher_id = sdp.nk_publisher_id 
    left join stg__dim_book_language sdbl
        on sdb.nk_language_id = sdbl.nk_language_id
),
final_dim_book as(
select 
    {{ dbt_utils.generate_surrogate_key( ["nk_book_id"] ) }} as sk_book_id
    , *
from dim_book
)

select * from final_dim_book

{% endsnapshot %}