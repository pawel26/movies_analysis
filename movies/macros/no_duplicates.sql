{% macro check_for_duplicates(model, columns) %}
    with duplicate_check as (
        select
            {{ columns | join(', ') }},
            count(*) as count
        from {{ ref(model) }}
        group by {{ columns | join(', ') }}
        having count(*) > 1
    )

    select *
    from duplicate_check
{% endmacro %}