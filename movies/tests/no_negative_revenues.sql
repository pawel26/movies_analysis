SELECT *
FROM {{ ref('revenue_from_release') }}
WHERE total_revenue_since_release < 0