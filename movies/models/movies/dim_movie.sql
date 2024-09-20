select
    row_number() over () as movie_id,
    title,
    year,
    rated,
    released::date as released,
    runtime,
    director,
    writer,
    languages,
    countries,
    awards,
    ratings
from {{ source('dev', 'raw_source') }}