WITH ranked_movies AS (
    SELECT DISTINCT
        m.movie_id,
        m.title,
        MAX(m.imdb_rating) AS imdb_rating,
        MAX(m.imdb_votes) AS imdb_votes,
        RANK() OVER (ORDER BY MAX(m.imdb_votes) DESC) AS vote_rank
    FROM
        ref('fct_movie_revenue') f
    JOIN
        ref('dim_movie') m
    ON
        f.movie_id = m.movie_id
    WHERE
        m.imdb_rating IS NOT NULL
        AND m.imdb_votes IS NOT NULL
    GROUP BY
        m.movie_id, m.title
)
SELECT
    DISTINCT
    title,
    imdb_rating,
    imdb_votes,
    vote_rank
FROM
    ranked_movies
ORDER BY
    imdb_rating DESC,
    vote_rank;