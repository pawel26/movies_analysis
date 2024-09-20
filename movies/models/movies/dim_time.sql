with dates as (
    select
        distinct date as date
    from {{ ref('raw_source') }}
)
select
    row_number() over () as date_id,
    date,
    extract(day from date) as day,
    extract(month from date) as month,
    extract(year from date) as year,
    extract(quarter from date) as quarter,
    extract(week from date) as week,
    extract(dow from date) as day_of_week
from dates