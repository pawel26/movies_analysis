SELECT
    d.name AS distributor_name,
    SUM(m.imdb_votes) AS total_votes
FROM
    ref('fct_movie_revenue') f
JOIN
    ref('dim_movie') m
ON
    f.movie_id = m.movie_id
JOIN
    dev.dim_distributor d
ON
    f.distributor_id = d.distributor_id
WHERE
    d.name <> 'not provided'
    AND m.imdb_votes IS NOT NULL
GROUP BY
    d.name
HAVING
    SUM(m.imdb_votes) > 0
ORDER BY
    total_votes DESC;