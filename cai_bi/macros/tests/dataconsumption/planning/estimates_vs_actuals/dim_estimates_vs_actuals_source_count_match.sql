{% test dim_estimates_vs_actuals_source_count_match(model, source) %}
    with
        eva as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from eva sfc
        where sfc.src_sys_key = 'sfc'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}