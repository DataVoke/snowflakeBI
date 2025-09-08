{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_rate_card_set"
    )
}}


with 
    silver_rcs as (
        select * from {{ ref('sales_rate_card_set') }} where src_sys_key = 'sfc' 
    ),
    ukg_employees as (
        select ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id, ifnull(sfc.key, por.contact_id) as sfc_contact_id, ukg.* 
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }} as sfc on ukg.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }} as por on ukg.hash_link = por.hash_link and por.src_sys_key = 'por'
        where ukg.src_sys_key = 'ukg'
    ),
    sfc_employees as (
        select sfc.salesforce_user_id as sfc_user_id, sfc.key as sfc_contact_id, sfc.* 
        from {{ ref('employee') }} sfc
        where sfc.src_sys_key = 'sfc' and sfc.key not in (select sfc_contact_id from ukg_employees where sfc_contact_id is not null) 
    ),
    all_employees as (
        select * from ukg_employees 
        union
        select * from sfc_employees
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    silver_rcs.key as key,
    all_employees.key as key_owner,
    silver_rcs.src_created_by_id,
    silver_rcs.src_modified_by_id,
    silver_rcs.currency_iso_code,
    silver_rcs.bln_filter_by_dates,
    silver_rcs.bln_is_active,
    silver_rcs.dte_expiration,
    silver_rcs.dts_src_created,
    silver_rcs.dts_src_modified,
    silver_rcs.dts_system_modstamp,
    silver_rcs.name,
    silver_rcs.type,
    silver_rcs.key_owner as sfc_owner_id,
    all_employees.email_address_work as owner_email,
    all_employees.display_name as owner_name,
    all_employees.display_name_lf as owner_name_lf
from silver_rcs
left join all_employees on silver_rcs.key_owner = all_employees.sfc_user_id