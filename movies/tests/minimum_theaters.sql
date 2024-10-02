with invalid_theaters as (
    select *
    from {{ ref('fct_movie_revenue') }}
    where theaters < 0
)

select * from invalid_theaters