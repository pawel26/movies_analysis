select
    t.date_id,
    m.movie_id,
    r.revenue,
    r.theaters,
    r.distributor
from {{ ref('raw_source') }} r
join {{ ref('dim_movie') }} m
on r.title = m.title
join {{ ref('dim_time') }} t
on r.date = t.date