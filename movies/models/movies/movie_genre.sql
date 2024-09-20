with split_genres as (
    select
        title,
        unnest(string_to_array(genre, ', ')) as genre
    from {{ ref('raw_source') }}
),
genre_ids as (
    select
        genre_id,
        genre
    from {{ ref('dim_genre') }}
),
movie_ids as (
    select
        movie_id,
        title
    from {{ ref('dim_movie') }}
)
select
    m.movie_id,
    g.genre_id
from split_genres s
join genre_ids g
on s.genre = g.genre
join movie_ids m
on s.title = m.title