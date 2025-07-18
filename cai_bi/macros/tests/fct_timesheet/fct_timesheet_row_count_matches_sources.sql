{% test fct_timesheet_row_count_matches_sources(model, source) %}
    with
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select count(*) as cnt from {{ source }} where src_sys_key = 'int'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}
