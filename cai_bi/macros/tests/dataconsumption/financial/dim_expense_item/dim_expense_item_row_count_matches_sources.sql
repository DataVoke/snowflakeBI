{% test dim_expense_item_row_count_matches_sources(model, source1, source2) %}
    with
        int_expense_item as (
            select * 
            from {{ source1 }} 
            where src_sys_key = 'int' and bln_line_item = true
        ),
        sfc_expense_item as (
            select * 
            from {{ source1 }}
            where src_sys_key = 'sfc'
        ),
        int_expense as (
            select * 
            from {{ source2 }}
            where src_sys_key = 'int'
        ),
        sfc_expense as (
            select * 
            from {{ source2 }}
            where src_sys_key = 'sfc'
        ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from int_expense_item int_ei
        left join sfc_expense_item sfc_ei on int_ei.hash_link = sfc_ei.hash_link
        left join int_expense int_e on int_ei.key_expense = int_e.key
        left join sfc_expense sfc_e on int_e.hash_link = sfc_e.hash_link
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}