{% test dim_project_row_count_matches_sources(model, source) %}
    with
    int_source as (select * from {{ source }} where src_sys_key = 'int'),
    pts_source as (select * from {{ source }} where src_sys_key = 'psa'),
    sfc_source as (select * from {{ source }} where src_sys_key = 'sfc'),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select count(*) as cnt from int_source
        left join pts_source on int_source.hash_link = pts_source.hash_link
        left join sfc_source on int_source.hash_link = sfc_source.hash_link
        where lower(int_source.project_type) <> 'client site'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}
