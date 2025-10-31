{% test dim_employee_row_count_matches_sources(model, source) %}
    with
        ukg_source as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from ukg_source ukg
        left join ukg_source sin on ukg.link = sin.link and sin.src_sys_key = 'int'
        left join ukg_source por on ukg.link = por.link and por.src_sys_key = 'por'
        left join ukg_source sfc on ukg.link = sfc.link and sfc.src_sys_key = 'sfc'
        where ukg.src_sys_key = 'ukg'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}