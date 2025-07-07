with stg__dim_date as(
    {{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
),
final_dim_date as(
    select
        {{ dbt_utils.generate_surrogate_key( ["date_day"] ) }} as sk_date_id
        , *
    from stg__dim_date
)
select * from final_dim_date
