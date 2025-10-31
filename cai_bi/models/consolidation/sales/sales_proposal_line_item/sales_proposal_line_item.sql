{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_proposal_line_item"
    )
}}

with
    sfc_proposal_line_item as (select * from {{ source("salesforce", "quote_line_item") }} where _fivetran_deleted = false),

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
        quote_id as key_proposal,
        md5(quote_id) as hash_key_proposal,
        opportunity_line_item_id as key_opportunity_line_item,
        md5(opportunity_line_item_id) as hash_key_opportunity_line_item,
        product_2_id as key_product,
        md5(product_2_id) as hash_key_product,
        pricebook_entry_id as key_pricebook_entry,
        md5(pricebook_entry_id) as hash_key_pricebook_entry,
        practice_c as key_practice,
        md5(practice_c) as hash_key_practice,
        practice_area_c as key_practice_area,
        md5(practice_area_c) as hash_key_practice_area,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        currency_iso_code as currency_iso_code,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        line_number as line_number,
        cast(quantity as number(38,0)) as qty,
        sort_order as sort_order,
        total_price_location_c as total_price_location,
        total_price_opportunity_c as total_price_opportunity,
        unit_price as unit_price,
        subtotal as subtotal,
        total_price as total_price,
        list_price as list_price
    from sfc_proposal_line_item
) select * from final
