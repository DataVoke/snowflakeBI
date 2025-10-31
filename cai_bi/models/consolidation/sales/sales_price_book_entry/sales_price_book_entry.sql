{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_price_book_entry"
    )
}}

with
    sfc_price_book_entry as (select * from {{ source("salesforce", "pricebook_entry") }} where _fivetran_deleted = false),

final as (
    select
        'sfc' as src_sys_key,
        cast(current_timestamp as timestamp_tz) as dts_created_at,
        '{{ this.name }}' as created_by,
        cast(current_timestamp as timestamp_tz) as dts_updated_at,
        '{{ this.name }}' as updated_by,
        cast(current_timestamp as timestamp_tz) as dts_eff_start,
        cast('9999-12-31' as timestamp_tz ) as dts_eff_end,
        true as bln_current,
        id as key,
        md5(id) as hash_key,
        id as link,
        md5(id) as hash_link,
        product_2_id as key_product,
        md5(product_2_id) as hash_key_product,
        pricebook_2_id as key_price_book_entry,
        md5(pricebook_2_id) as hash_key_price_book_entry,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        currency_iso_code as currency_iso_code,
        is_active as bln_is_active,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        name as name,
        unit_price as unit_price,
        use_standard_price as use_standard_price,
    from sfc_price_book_entry
) select * from final
