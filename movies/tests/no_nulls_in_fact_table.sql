WITH null_check AS (
    SELECT
        movie_id,
        date_id,
        distributor_id,
        revenue,
        theaters,
        released
    FROM {{ ref('fct_movie_revenue') }}
    WHERE
        movie_id IS NULL OR
        date_id IS NULL OR
        distributor_id IS NULL OR
        revenue IS NULL OR
        theaters IS NULL OR
        released IS NULL
)

SELECT
    COUNT(*) AS null_count
FROM null_check
HAVING COUNT(*) = 0
