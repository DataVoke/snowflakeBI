{% test cc_transaction_entry_source_count_match_int(model) %}
    with model_count as (
        select count(*) as cnt
        from {{ model }}
        where src_sys_key = 'int'
    ),
    source_count as (
        select count(*) as cnt
        from {{ source('sage_intacct', 'cc_transaction_entry') }}
        where _fivetran_deleted = false
    )
    select 1
    where (select cnt from model_count) != (select cnt from source_count)
{% endtest %}
