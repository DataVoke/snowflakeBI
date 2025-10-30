{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_opportunity_team_member"
    )
}}

with 
    silver_oppty_tm as (
        select * from {{ ref('sales_opportunity_team_member') }} where src_sys_key = 'sfc' 
    ),
    silver_acct as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
    ),
    silver_oppty as (
        select * from {{ ref('sales_opportunity') }} where src_sys_key = 'sfc' 
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
    silver_oppty_tm.key as key,
    silver_oppty.key_account as key_account,
    all_employees.key as key_employee,
    silver_oppty_tm.key_opportunity,
    silver_oppty_tm.src_created_by_id,
    silver_oppty_tm.src_modified_by_id,
    silver_acct.name as account_name,
    silver_oppty_tm.currency_iso_code as currency_iso_code,
    silver_oppty_tm.dts_src_created as dts_src_created,
    silver_oppty_tm.dts_src_modified,
    silver_oppty_tm.dts_system_modstamp,
    all_employees.email_address_work as employee_email,
    all_employees.display_name as employee_name,
    all_employees.display_name_lf as employee_name_lf,
    silver_oppty_tm.name,
    silver_oppty_tm.opportunity_access_level,
    silver_oppty_tm.name as opportunity_name,
    silver_oppty_tm.key_user as sfc_user_id,
    silver_oppty_tm.team_member_role,
    ifnull(silver_oppty_tm.title, all_employees.job_title) as job_title
from silver_oppty_tm
left join silver_oppty as silver_oppty on silver_oppty_tm.key_opportunity = silver_oppty.key
left join silver_acct as silver_acct on silver_oppty.key_account = silver_acct.key
left join all_employees on silver_oppty_tm.key_user = all_employees.sfc_user_id