{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_phase_code_products"
    )
}}

select 
    pcf.record_id as key,
    pcf.id as product_id,
    pcf.display_name as name,
    pcf.description as description,
    pcf.visible as bln_is_visible,
    pcf.ukg_id,
    pcf.intacct_id,
    pcf.salesforce_id,
    pcf.certinia_id,
    pcf.entra_id
from prod_bi_dw.cai_psatools.phase_code_families as pcf
where pcf._fivetran_deleted = false