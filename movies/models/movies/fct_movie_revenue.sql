select
    t.date_id,
    m.movie_id,
    d.distributor_id,
    r.revenue,
    r.theaters,
    r.box_office,
    r.imdb_rating,
    r.imdb_votes
from {{ source('dev', 'raw_source') }} r
join {{ ref('dim_movie') }} m
on r.title = m.title
join {{ ref("dim_date") }} t
on r.date = t.date
join {{ ref('dim_distributor') }} d
on r.distributor = d.name