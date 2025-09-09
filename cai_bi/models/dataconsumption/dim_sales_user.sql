{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_user"
    )
}}

with 
    sf_users as (
        select * from {{ ref('sales_user') }} where src_sys_key = 'sfc' 
    ),
    sf_contacts as (
        select * from {{ ref('sales_contact') }} where src_sys_key = 'sfc' 
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
    sf_users.key as key,
    all_employees.key as key_employee,
    del_approver.key as key_delegated_approver,
    all_employees.supervisor_id as key_supervisor,
    sf_contacts.key as key_contact,
    sf_users.cx_client_id,
    sf_users.cx_project_id as cx_project_id,
    sf_users.src_created_by_id,
    sf_users.src_modified_by_id as src_modified_by_id,
    sf_users.user_role_id as user_role_id,
    sf_users.ukg_employee_id_c,
    sf_users.ukg_person_id,
    sf_users.key_delegated_approver as sfc_delegated_approver_id,
    sf_users.key_supervisor as sfc_supervisor_id,
    sf_users.alias,
    sf_users.amt_amr_closed_won_goal,
    sf_users.amt_closed_won_goal,
    sf_users.amt_cqv_aut_closed_won_goal,
    sf_users.amt_dc_closed_won_goal,
    sf_users.amt_hps_closed_won_goal,
    sf_users.amt_pmt_closed_won_goal,
    sf_users.amt_ppm_closed_won_goal,
    sf_users.amt_qcr_closed_won_goal,
    sf_users.amt_submitted_proposal_goal,
    sf_users.amt_total_price_submitted,
    sf_users.amt_total_price_won,
    sf_users.bln_cx_internal,
    sf_users.bln_forecast_enabled,
    sf_users.bln_has_sales_quota,
    sf_users.bln_has_user_verified_email,
    sf_users.bln_has_user_verified_phone,
    sf_users.bln_is_active,
    sf_users.bln_is_profile_photo_active,
    sf_users.bln_receives_admin_info_emails,
    sf_users.bln_receives_info_emails,
    sf_users.business_area,
    sf_users.community_nickname,
    sf_users.company_name,
    sf_users.country,
    sf_users.country_code,
    sf_users.currency_iso_code,
    sf_users.cx_client_name,
    sf_users.cx_project_name,
    sf_users.db_region,
    sf_users.default_currency_iso_code as default_currency_iso_code,
    sf_users.default_group_notification_frequency as default_group_notification_frequency,
    ifnull(del_approver.display_name, del_approver_sf.name) as delegate_approver_name,
    ifnull(del_approver.display_name_lf, del_approver_sf.name) as delegate_approver_name_lf,
    ifnull(del_approver.email_address_work, del_approver_sf.email) as delegate_approver_email,
    sf_users.department,
    sf_users.digest_frequency,
    ifnull(all_employees.display_name, sf_users.name) as display_name,
    ifnull(all_employees.display_name_lf, sf_users.name) as display_name_lf,
    sf_users.division,
    sf_users.dts_last_login_date,
    sf_users.dts_password_expiration_date,
    sf_users.dts_src_created,
    sf_users.dts_src_modified,
    sf_users.dts_system_modstamp,
    sf_users.email,
    sf_users.email_encoding_key,
    sf_users.employee_number,
    sf_users.end_day,
    sf_users.federation_identifier,
    sf_users.first_name,
    sf_users.language_locale_key,
    sf_users.last_name,
    sf_users.locale_sid_key,
    sf_users.middle_name,
    sf_users.mobile_phone,
    sf_users.name,
    sf_users.phone,
    sf_users.profile_id,
    sf_users.signature,
    sf_users.start_day,
    sf_users.state,
    sf_users.state_code,
    sf_users.time_zone_sid_key,
    sf_users.title,
    sf_users.ukg_employee_number,
    sf_users.user_type,
    sf_users.user_name
from sf_users
left join sf_contacts on sf_users.key = sf_contacts.key_user
left join all_employees on sf_users.ukg_employee_id_c = all_employees.key
left join all_employees as del_approver on sf_users.key_delegated_approver = del_approver.sfc_user_id
left join sf_users as del_approver_sf on sf_users.key_delegated_approver = del_approver_sf.key
