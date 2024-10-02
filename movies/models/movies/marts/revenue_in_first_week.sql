SELECT DISTINCT
    m.movie_id,
    m.title,
    f.released,
    SUM(f.revenue) AS total_revenue_first_week
FROM {{  ref('fct_movie_revenue') }} f
JOIN {{  ref('dim_movie') }} m
    ON f.movie_id = m.movie_id
JOIN {{  ref('dim_date') }} d
    ON f.date_id = d.date_id
WHERE d.date BETWEEN f.released::date AND (f.released::date + INTERVAL '7 days')
GROUP BY m.movie_id, m.title, f.released
ORDER BY total_revenue_first_week DESC