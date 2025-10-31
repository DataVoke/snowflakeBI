{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_product"
    )
}}

with
    sfc_product as (select * from {{ source("salesforce", "product_2") }} where _fivetran_deleted = false),

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
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        currency_iso_code as currency_iso_code,
        is_active as bln_is_active,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        name as name
    from sfc_product
) select * from final
