select
    row_number() over () as movie_id,
    title,
    year,
    rated,
    released::date as released,
    runtime,
    director,
    writer,
    language,
    country,
    awards,
    box_office
from {{ ref('raw_source') }}