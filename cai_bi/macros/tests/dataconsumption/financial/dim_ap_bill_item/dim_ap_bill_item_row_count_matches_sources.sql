{% test dim_ap_bill_item_row_count_matches_sources(model, apbillitem, apbill) %}
    with
    ap_bill_item as (
        select * 
        from {{ apbillitem }} 
        where src_sys_key = 'int' and bln_line_item = true
    ),
    ap_bill as (
        select * 
        from {{ apbill }} 
    ),
    model_count as (
        select count(*) as cnt from {{ model }}
    ),
    source_count as (
        select 
            count(*) as cnt 
        from ap_bill_item abi
        left join ap_bill ab on abi.key_ap_bill = ab.key
        where ab.key is not null
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}