with distinct_movies as (
    select distinct on (title)
        title,
        year,
        rated,
        case
            when released = 'N/A' then NULL
            else released::date
        end as released,
        runtime,
        director,
        writer,
        languages,
        countries,
        awards,
        ratings,
        box_office,
        imdb_rating,
        imdb_votes
    from dev.raw_source
    order by title, released
)
select
    row_number() over () as movie_id,
    title,
    year,
    rated,
    released,
    runtime,
    director,
    writer,
    languages,
    countries,
    awards,
    ratings,
    box_office,
    imdb_rating,
    imdb_votes
from distinct_movies