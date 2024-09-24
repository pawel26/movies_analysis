WITH genre_votes AS (
    SELECT
        g.genre,
        SUM(m.imdb_votes) AS total_votes
    FROM
        ref('dim_movie') m
    JOIN
        ref('fct_movie_genre') mg
    ON
        m.movie_id = mg.movie_id
    JOIN
        dev.dim_genre g
    ON
        mg.genre_id = g.genre_id
    WHERE genre <> 'N/A'
    GROUP BY
        g.genre
)
SELECT
    genre,
    total_votes,
    RANK() OVER (ORDER BY total_votes DESC) AS genre_rank
FROM
    genre_votes
ORDER BY
    total_votes DESC;