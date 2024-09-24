SELECT
    DATE_PART('year', d.date)::INTEGER AS year,
    SUM(f.revenue) AS total_revenue
FROM
    ref('fct_movie_revenue') f
JOIN
    ref('dim_date') d
ON
    f.date_id = d.date_id
GROUP BY
    DATE_PART('year', d.date)
ORDER BY
    total_revenue DESC
LIMIT 1;