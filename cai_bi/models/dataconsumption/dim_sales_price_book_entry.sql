{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_price_book_entry"
    )
}}

with 
    silver_pbe as (
        select * from {{ ref('sales_price_book_entry') }} where src_sys_key = 'sfc' 
    ),
    silver_products as (
        select * from {{ ref('sales_product') }} where src_sys_key = 'sfc' 
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    silver_pbe.key as key,
    silver_pbe.key_product as key_product,
    silver_pbe.key_price_book_entry as key_price_book_entry,
    silver_pbe.src_created_by_id,
    silver_pbe.src_modified_by_id,
    silver_pbe.currency_iso_code as currency_iso_code,
    silver_pbe.dts_src_created as dts_src_created,
    silver_pbe.dts_src_modified,
    silver_pbe.dts_system_modstamp,
    silver_pbe.name,
    silver_pbe.bln_is_active,
    silver_pbe.unit_price,
    silver_pbe.use_standard_price,
    silver_products.name as product_name
from silver_pbe
left join silver_products on silver_pbe.key_product = silver_products.key