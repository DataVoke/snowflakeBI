{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_product"
    )
}}

with 
    silver_product as (
        select * from {{ ref('sales_product') }} where src_sys_key = 'sfc' 
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    silver_product.key as key,
    silver_product.src_created_by_id,
    silver_product.src_modified_by_id,
    silver_product.currency_iso_code as currency_iso_code,
    silver_product.dts_src_created as dts_src_created,
    silver_product.dts_src_modified,
    silver_product.dts_system_modstamp,
    silver_product.name,
    silver_product.bln_is_active
from silver_product