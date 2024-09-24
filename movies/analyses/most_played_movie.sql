SELECT
    m.title,
    MAX(f.theaters) AS max_theaters
FROM
    {{  ref('fct_movie_revenue')}} f
JOIN
    dev.dim_movie m
ON
    f.movie_id = m.movie_id
GROUP BY
    m.title
ORDER BY
    max_theaters DESC
LIMIT 1;