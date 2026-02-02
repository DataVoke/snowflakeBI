{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_phase_code_products_links"
    )
}}

select 
    pcfl.record_id as key,
    pcf.record_id as key_product,
    emp_por_created.key as key_created_by,
    emp_por_verified.key as key_verified_by,
    date(pcfl.dts_created) as dte_created,
    emp_por_created.display_name as created_by_name,
    emp_por_created.display_name_lf as created_by_name_lf,
    pcfl.id as id,
    pcf.display_name as product_name,
    pcf.description as product_description,
    pcf.visible as bln_product_is_visible,
    pcfl.phase_code as phase_code_name,
    pcfl.bln_verified,
    date(pcfl.dts_verified_by) as dte_verified,
    emp_por_verified.display_name as verified_by_name,
    emp_por_verified.display_name_lf as verified_by_name_lf
from {{ source("psatools","phase_code_families_links") }} as pcfl
left join {{ source("psatools","phase_code_families") }} as pcf on pcfl.phase_code_family_id = pcf.id
left join {{ ref('dim_employee') }} as emp_por_created on pcfl.created_by_id = emp_por_created.portal_id
left join {{ ref('dim_employee') }} as emp_por_verified on pcfl.verified_by_id = emp_por_verified.portal_id
where pcfl._fivetran_deleted = false and pcf._fivetran_deleted = false