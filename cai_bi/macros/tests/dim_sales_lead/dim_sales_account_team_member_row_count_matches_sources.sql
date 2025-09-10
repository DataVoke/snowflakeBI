{% test dim_sales_lead_row_count_matches_sources(model, source) %}
    with
        sales_lead_source as (select * from {{ source }} ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from sales_lead_source sfc
        where sfc.src_sys_key = 'sfc'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}