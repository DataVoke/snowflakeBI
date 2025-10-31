{% test dim_project_task_row_count_matches_sources(model, source) %}
    with
        project_task as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from project_task sfc
        where sfc.src_sys_key = 'sfc'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}