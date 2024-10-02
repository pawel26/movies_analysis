SELECT DISTINCT
    m.movie_id,
    m.title,
    f.released,
    SUM(f.revenue) AS total_revenue_since_release
FROM {{  ref('fct_movie_revenue') }} f
JOIN {{  ref('dim_movie') }} m
    ON f.movie_id = m.movie_id
JOIN {{  ref('dim_date') }} d
    ON f.date_id = d.date_id
WHERE d.date >= f.released::date
GROUP BY m.movie_id, m.title, f.released
ORDER BY total_revenue_since_release DESC