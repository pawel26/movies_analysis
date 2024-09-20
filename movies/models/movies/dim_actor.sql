with split_actors as (
    select
        unnest(string_to_array(actors, ', ')) as actor
    from {{ ref('raw_source') }}
)
select
    row_number() over () as actor_id,
    actor
from split_actors
group by actor