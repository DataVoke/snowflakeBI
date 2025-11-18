{% test dim_employee_compensation_row_count_matches_sources(model, source) %}
    with
        ukg_source as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select count(*) as cnt from ukg_source ukg where ukg.src_sys_key = 'ukg'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}