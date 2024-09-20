with distributors as (
    select
        distinct distributor as name
    from {{ source('dev', 'raw_source') }}
)
select
    row_number() over () as distributor_id,
    name
from distributors