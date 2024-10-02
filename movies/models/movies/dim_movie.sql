{{
  config(
    materialized='incremental',
    unique_key='movie_id',
    incremental_strategy='merge'
  )
}}

WITH distinct_movies AS (
    SELECT DISTINCT ON (title)
        title,
        year,
        rated,
        runtime,
        director,
        writer,
        languages,
        countries,
        awards,
        ratings,
        box_office,
        imdb_rating,
        imdb_votes,
        string_to_array(actors, ', ') AS actors_array,
        string_to_array(genre, ', ') AS genres_array
    FROM {{ source('dev', 'raw_source') }}
),

final_movies AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['title']) }} AS movie_id,
        title,
        year,
        rated,
        runtime,
        director,
        writer,
        languages,
        countries,
        awards,
        ratings,
        box_office,
        imdb_rating,
        imdb_votes,
        genres_array AS genres,
        actors_array AS actors
    FROM distinct_movies
    WHERE
        imdb_rating IS NOT NULL
        AND imdb_votes IS NOT NULL
)

SELECT
    final_movies.movie_id,
    final_movies.title,
    final_movies.year,
    final_movies.rated,
    final_movies.runtime,
    final_movies.director,
    final_movies.writer,
    final_movies.languages,
    final_movies.countries,
    final_movies.awards,
    final_movies.ratings,
    final_movies.box_office,
    final_movies.imdb_rating,
    final_movies.imdb_votes,
    final_movies.genres,
    final_movies.actors,
    width_bucket(final_movies.imdb_votes, 1000, 50000, 5) AS vote_bucket,
    width_bucket(final_movies.imdb_rating, 1, 10, 5) AS rating_bucket
FROM final_movies
{% if is_incremental() %}
LEFT JOIN {{ this }} AS existing_movies
    ON final_movies.movie_id = existing_movies.movie_id
WHERE existing_movies.movie_id IS NULL
OR final_movies.movie_id != existing_movies.movie_id
{% endif %}