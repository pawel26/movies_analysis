{% set model = 'fct_movie_revenue' %}
{% set columns = ['movie_id', 'date_id', 'distributor_id'] %}

{{ check_for_duplicates(model, columns) }}