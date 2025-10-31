{{ config(
    materialized = "table",
    schema = "dataconsumption",
    alias="sales_account_team_member"
) }}

with 
    silver_acct as (
        select * from {{ ref('sales_account_team_member') }} where src_sys_key = 'sfc' 
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
    ),
    silver_sales_account as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc'
    )

    select 
        silver_acct.key,
        silver_acct.key_account,
        all_employees.key as key_employee,
        silver_acct.src_created_by_id,
        silver_acct.src_modified_by_id,
        silver_acct.account_access_level,
        silver_sales_account.name as account_name,
        silver_acct.case_access_level,
        silver_acct.contact_access_level,
        silver_acct.currency_iso_code,
        silver_acct.dts_src_created as dts_src_created,
        silver_acct.dts_src_modified,
        silver_acct.dts_system_modstamp,
        silver_acct.opportunity_access_level as opportunity_access_level,
        silver_acct.key_user as sfc_user_id,
        silver_acct.team_member_role as team_member_role,
        ifnull(silver_acct.title, all_employees.job_title) as job_title,
        all_employees.email_address_work as employee_email,
        all_employees.display_name as employee_name,
        all_employees.display_name_lf as employee_name_lf
    from silver_acct
    left join silver_sales_account on silver_acct.key_account = silver_sales_account.key
    left join all_employees on silver_acct.key_user = all_employees.sfc_user_id