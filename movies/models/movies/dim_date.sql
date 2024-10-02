{{
  config(
    materialized = 'incremental',
    unique_key = 'date_id'
  )
}}

WITH dates AS (
    SELECT DISTINCT
        date::date AS date
    FROM {{ source('dev', 'raw_source') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_id,
    date,
    EXTRACT(day FROM date) AS day,
    EXTRACT(month FROM date) AS month,
    EXTRACT(year FROM date) AS year,
    EXTRACT(quarter FROM date) AS quarter,
    EXTRACT(week FROM date) AS week,
    EXTRACT(dow FROM date) AS day_of_week,
    CASE
        WHEN EXTRACT(dow FROM date) IN (6, 7) THEN 1  -- Sobota (6) lub Niedziela (7)
        ELSE 0
    END AS is_weekend
FROM dates

{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['date']) }} NOT IN (
        SELECT date_id FROM {{ this }}
    )
{% endif %}