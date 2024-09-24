SELECT
    a.actor,
    COUNT(DISTINCT m.title) AS count_movies,
    RANK() OVER (ORDER BY COUNT(DISTINCT m.title) DESC) AS actor_rank
FROM
    ref('fct_movie_actor') f
JOIN
    ref('dim_movie') m
ON
    f.movie_id = m.movie_id
JOIN
    dev.dim_actor a
ON
    f.actor_id = a.actor_id
WHERE a.actor <> 'N/A'
GROUP BY
    a.actor
ORDER BY
    count_movies DESC;    