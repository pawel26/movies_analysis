{% set model = 'dim_movie' %}
{% set columns = ['movie_id', 'title', 'runtime'] %}

{{ check_for_duplicates(model, columns) }}