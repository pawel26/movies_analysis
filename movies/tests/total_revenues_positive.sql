SELECT SUM(total_revenue_since_release)
FROM {{ ref('revenue_from_release') }}
HAVING SUM(total_revenue_since_release) <= 0
