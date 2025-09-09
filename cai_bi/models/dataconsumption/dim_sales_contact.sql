


with 
    sf_users as (
        select * from {{ ref('sales_user') }} where src_sys_key = 'sfc' 
    ),
    sf_contacts as (
        select * from {{ ref('sales_contact') }} where src_sys_key = 'sfc' 
    ),
    sf_accts as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
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
    por_practices as (
        select * from {{ source('portal','practices') }} where _fivetran_deleted = false
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    sf_contacts.key,
    sf_contacts.key_account,
    sf_contacts.key_owner,
    por_practices.record_id as key_practice,
    all_employees.supervisor_id as key_supervisor,
    sf_contacts.key_user,
    all_employees.key as key_employee,
    sf_contacts.actuals_last_updated_by_id,
    sf_contacts.external_id,
    sf_contacts.master_record_id,
    sf_contacts.record_type_id,
    sf_contacts.key_practice as sfc_practice_id,
    sf_contacts.key_supervisor as sfc_supervisor_id,
    sf_contacts.key_user as sfc_user_id,
    sf_contacts.src_created_by_id,
    sf_contacts.src_modified_by_id,
    sf_contacts.utilization_last_updated_by_id,
    sf_contacts.work_calendar_id,
    sf_accts.name as account_name,
    sf_contacts.bln_is_active,
    sf_contacts.base_team,
    sf_contacts.billing_category,
    sf_contacts.biography,
    sf_contacts.bln_allow_timecards_without_assignment,
    sf_contacts.bln_do_not_call,
    sf_contacts.bln_exclude_from_resource_planner,
    sf_contacts.bln_has_opted_out_of_email,
    sf_contacts.bln_is_action_calculate_utilization,
    sf_contacts.bln_is_action_update_current_time_period,
    sf_contacts.bln_is_email_bounced,
    sf_contacts.bln_is_external_resource,
    sf_contacts.bln_is_part_time_resource,
    sf_contacts.bln_is_qualified_project_manager,
    sf_contacts.bln_is_resource,
    sf_contacts.bln_is_resource_active,
    sf_contacts.bln_is_synced_to_marketo,
    sf_contacts.blog_subscription,
    sf_contacts.conferenceor_show,
    sf_contacts.currency_iso_code,
    sf_contacts.department,
    sf_contacts.description,
    sf_contacts.dte_last_activity,
    sf_contacts.dte_of_industry_experience,
    sf_contacts.dts_actuals_last_updated,
    sf_contacts.dts_email_bounced,
    sf_contacts.dts_legacy_created,
    sf_contacts.dts_marketo_acquisition,
    sf_contacts.dts_src_created,
    sf_contacts.dts_src_modified,
    sf_contacts.dts_system_modstamp,
    sf_contacts.dts_utilization_last_updated,
    sf_contacts.email,
    sf_contacts.email_bounced_reason,
    all_employees.display_name as employee_name,
    all_employees.display_name_lf as employee_name_lf,
    all_employees.email_address_work as employee_email,
    sf_contacts.employee_pay_type,
    sf_contacts.employee_type,
    sf_contacts.fax,
    sf_contacts.first_name,
    sf_contacts.historical_utilization_target_hours,
    sf_contacts.home_phone,
    sf_contacts.industry,
    sf_contacts.intacct_employee_id,
    sf_contacts.interested_business_areas,
    sf_contacts.last_name,
    sf_contacts.lead_source,
    sf_contacts.legacy_lead_source,
    sf_contacts.legal_basis_processing_data,
    sf_contacts.linked_in_profile_url,
    sf_contacts.mailing_city,
    sf_contacts.mailing_country,
    sf_contacts.mailing_country_code,
    sf_contacts.mailing_postal_code,
    sf_contacts.mailing_state,
    sf_contacts.mailing_state_code,
    sf_contacts.mailing_street,
    sf_contacts.marketo_acquisition_program,
    sf_contacts.marketo_lead_score,
    sf_contacts.middle_name,
    sf_contacts.mobile_phone,
    sf_contacts.name,
    sf_contacts.original_source,
    sf_users.name as owner_name,
    sf_contacts.phone,
    por_practices.display_name as practice_name,
    sf_contacts.pse_group,
    sf_contacts.pse_last_date,
    sf_contacts.pse_start_date,
    sf_contacts.resource_role,
    sf_contacts.salutation,
    supervisor.display_name as supervisor_name,
    sf_contacts.suffix,
    sf_contacts.title
from sf_contacts
left join sf_users on sf_contacts.key_user = sf_users.key
left join sf_accts on sf_contacts.key_account = sf_accts.key
left join all_employees on sf_users.ukg_employee_id_c = all_employees.key
left join all_employees as supervisor on all_employees.supervisor_id = supervisor.key
left join por_practices on sf_contacts.key_practice = por_practices.salesforce_id