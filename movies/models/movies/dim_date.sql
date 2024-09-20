with dates as (
    select
        distinct date::date as date
    from {{ source('dev', 'raw_source') }}
)
select
    row_number() over () as date_id,
    date,
    extract(day from date) as day,
    extract(month from date) as month,
    extract(year from date) as year,
    extract(quarter from date) as quarter,
    extract(week from date) as week,
    extract(dow from date) as day_of_week,
    case
        when extract(dow from date) in (6, 7) then 1  -- Sobota (6) lub Niedziela (7)
        else 0
    end as is_weekend
from dates