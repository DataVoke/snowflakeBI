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
        select ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id, por.salesforce_user_id, ifnull(sfc.key, por.contact_id) as sfc_contact_id, ukg.* 
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }} as sfc on ukg.link = sfc.link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }} as por on ukg.link = por.link and por.src_sys_key = 'por'
        where ukg.src_sys_key = 'ukg'
    ),
    silver_sales_account as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc'
    )

    select 
        silver_acct.key,
        silver_acct.key_account,
        ukg_employees.key as key_user,
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
        silver_acct.title,
        ukg_employees.email_address_work as user_email_address,
        ukg_employees.display_name as user_name,
        ukg_employees.display_name_lf as user_name_lf
    from silver_acct
    left join silver_sales_account on silver_acct.key_account = silver_sales_account.key
    left join ukg_employees on silver_acct.key_user = ukg_employees.sfc_user_id