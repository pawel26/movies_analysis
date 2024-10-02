{{
  config(
    materialized = 'incremental',
    unique_key = 'distributor_id'
  )
}}

WITH distributors AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['distributor']) }} AS distributor_id,
        distributor AS name
    FROM {{ source('dev', 'raw_source') }}
)

SELECT
    distributor_id,
    name
FROM distributors
{% if is_incremental() %}
    WHERE distributor_id NOT IN (
        SELECT distributor_id FROM {{ this }}
    )
{% endif %}