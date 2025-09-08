{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_rate_card"
    )
}}

with 
    silver_rc as (
        select * from {{ ref('sales_rate_card') }} where src_sys_key = 'sfc' 
    ),
    silver_rcs as (
        select * from {{ ref('sales_rate_card_set') }} where src_sys_key = 'sfc' 
    ),
    silver_acct as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
    ),
    por_practices as (
        select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false
    ),
    por_locations as (
        select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false
    ),
    por_payroll_companies as (
        select * from {{ source('portal', 'payroll_companies') }} where _fivetran_deleted = false
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
    silver_rc.key as key,
    silver_rc.key_rate_card_set as key_rate_card_set,
    all_employees.key as key_owner,
    por_practices.record_id as key_practice,
    silver_rc.key_account as key_account,
    por_payroll_companies.record_id as key_payroll_company,
    por_locations.record_id as key_location,
    silver_rc.src_created_by_id,
    silver_rc.src_modified_by_id,
    silver_rc.billing_category,
    silver_rc.currency_iso_code,
    silver_rc.dte_end,
    silver_rc.dte_start,
    silver_rc.dts_src_created,
    silver_rc.dts_src_modified,
    silver_rc.dts_system_modstamp,
    silver_rc.name,
    ifnull(silver_rc.rate_all_inclusive,0) as rate_all_inclusive,
    ifnull(silver_rc.rate_suggested,0) as rate,
    silver_rc.role,
    por_practices.display_name as practice_name,
    silver_acct.name as account_name,
    por_payroll_companies.display_name as payroll_company_name,
    por_locations.display_name as location_name,
    silver_rcs.name as rate_card_set_name,
    all_employees.email_address_work as owner_email,
    all_employees.display_name as owner_name,
    all_employees.display_name_lf as owner_name_lf,
    silver_rc.key_practice as sfc_practice_id,
    silver_rc.key_group as sfc_group_id,
    silver_rc.key_location as sfc_location_id,
    silver_rc.key_owner as sfc_owner_id
from silver_rc
left join all_employees on silver_rc.key_owner = all_employees.sfc_user_id
left join silver_rcs on silver_rc.key_rate_card_set = silver_rcs.key
left join silver_acct on silver_rc.key_account = silver_acct.key
left join por_locations on silver_rc.key_location = por_locations.salesforce_id
left join por_practices on silver_rc.key_practice = por_practices.salesforce_id
left join por_payroll_companies on silver_rc.key_group = por_payroll_companies.salesforce_id