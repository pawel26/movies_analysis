with split_genres as (
    select
        unnest(string_to_array(genre, ', ')) as genre
    from {{ ref('raw_source') }}
)
select
    row_number() over () as genre_id,
    genre
from split_genres
group by genre