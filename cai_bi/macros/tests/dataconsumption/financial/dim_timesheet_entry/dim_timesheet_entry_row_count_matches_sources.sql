{% test dim_timesheet_entry_row_count_matches_sources(model, source) %}
    with
    int_source as (select * from {{ source }} where src_sys_key = 'int'),
    sfc_source as (
        select hash_link, sum(qty) as qty, listagg(notes, ', ') within group(order by notes) as notes
        from int_source
        where src_sys_key = 'sfc'
        group by hash_link
    ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select count(*) as cnt from int_source
        left join sfc_source on sfc_source.hash_link = int_source.hash_link
        where int_source.src_sys_key = 'int'
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}