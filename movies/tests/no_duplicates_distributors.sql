{% set model = 'dim_distributor' %}
{% set columns = ['distributor_id', 'name'] %}

{{ check_for_duplicates(model, columns) }}