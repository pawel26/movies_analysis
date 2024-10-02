{{ config(
    materialized='incremental',
    unique_key=['movie_id', 'date_id', 'distributor_id']
) }}

{% if is_incremental() %}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['r.title']) }} as movie_id,
    {{ dbt_utils.generate_surrogate_key(['r.date::date']) }} as date_id,
    {{ dbt_utils.generate_surrogate_key(['r.distributor']) }} as distributor_id,
    r.revenue,
    r.theaters,
    CASE
        WHEN r.released = 'N/A' THEN NULL
        ELSE r.released::date
    END AS released
FROM {{ source('dev', 'raw_source') }} r
JOIN {{ ref('dim_movie') }} m ON {{ dbt_utils.generate_surrogate_key(['r.title']) }} = m.movie_id
JOIN {{ ref('dim_date') }} t ON {{ dbt_utils.generate_surrogate_key(['r.date::date']) }} = t.date_id
JOIN {{ ref('dim_distributor') }} d ON {{ dbt_utils.generate_surrogate_key(['r.distributor']) }} = d.distributor_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }}
    WHERE {{ this }}.movie_id = m.movie_id
      AND {{ this }}.date_id = t.date_id
      AND {{ this }}.distributor_id = d.distributor_id
)

{% else %}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['r.title']) }} as movie_id,
    {{ dbt_utils.generate_surrogate_key(['r.date::date']) }} as date_id,
    {{ dbt_utils.generate_surrogate_key(['r.distributor']) }} as distributor_id,
    r.revenue,
    r.theaters,
    CASE
        WHEN r.released = 'N/A' THEN NULL
        ELSE r.released::date
    END AS released
FROM {{ source('dev', 'raw_source') }} r
JOIN {{ ref('dim_movie') }} m ON {{ dbt_utils.generate_surrogate_key(['r.title']) }} = m.movie_id
JOIN {{ ref('dim_date') }} t ON {{ dbt_utils.generate_surrogate_key(['r.date::date']) }} = t.date_id
JOIN {{ ref('dim_distributor') }} d ON {{ dbt_utils.generate_surrogate_key(['r.distributor']) }} = d.distributor_id

{% endif %}