SELECT
    m.title,
    SUM(f.revenue) AS total_revenue
FROM
    ref('fct_movie_revenue') f
JOIN
    ref('dim_movie') m
ON
    f.movie_id = m.movie_id
GROUP BY
    m.title
ORDER BY
    total_revenue DESC;