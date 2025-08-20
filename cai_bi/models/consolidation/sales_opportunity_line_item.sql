{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_opportunity_line_item"
    )
}}

with
    sfc_opportunity_line_item as (select * from {{ source("salesforce", "opportunity_line_item") }} where _fivetran_deleted = false),

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
        opportunity_id as key_opportunity,
        md5(opportunity_id) as hash_key_opportunity,
        business_area_c as key_practice_area,
        md5(business_area_c) as hash_key_practice_area,
        product_2_id as key_product,
        md5(product_2_id) as hash_key_product,
        pricebook_entry_id as key_price_book_entry,
        md5(pricebook_entry_id) as hash_key_price_book_entry,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        currency_iso_code as currency_iso_code,
        division_c as division,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        name as name,
        cast(quantity as number(38,0)) as qty,
        sort_order as sort_order,
        total_price_location_c as total_price_location,
        total_price_opportunity_c as total_price_opportunity,
        unit_price as unit_price,
    from sfc_opportunity_line_item
) select * from final
