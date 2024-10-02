with invalid_revenue as (
    select *
    from {{ ref('fct_movie_revenue') }}
    where revenue < 0
)

select * from invalid_revenue