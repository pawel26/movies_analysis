{% set model = 'dim_date' %}
{% set columns = ['date_id', 'date'] %}

{{ check_for_duplicates(model, columns) }}