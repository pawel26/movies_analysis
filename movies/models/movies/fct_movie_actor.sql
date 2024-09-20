with split_actors as (
    select
        title,
        unnest(string_to_array(actors, ', ')) as actor
    from {{ source('dev', 'raw_source') }}
),
actor_ids as (
    select
        actor_id,
        actor
    from {{ ref('dim_actor') }}
),
movie_ids as (
    select
        movie_id,
        title
    from {{ ref('dim_movie') }}
)
select
    m.movie_id,
    a.actor_id
from split_actors s
join actor_ids a
on s.actor = a.actor
join movie_ids m
on s.title = m.title