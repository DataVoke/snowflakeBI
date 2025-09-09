{% test dim_sales_user_row_count_matches_sources(model, source) %}
    with
       dim_sales_user_source as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from dim_sales_user_source sfc
        where sfc.src_sys_key = 'sfc'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}