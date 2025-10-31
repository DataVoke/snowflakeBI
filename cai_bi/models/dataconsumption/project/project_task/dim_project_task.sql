{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="project_task"
    )
}}

with 
    tasks as (select * from {{ ref('project_task') }} where src_sys_key='sfc'),
    project as (select * from {{ ref('project') }}),
    phase_codes as (select * from {{ ref('project_phase_code') }} where src_sys_key = 'sfc_milestones'),
    sf_accts as (select * from {{ ref('sales_account') }} where src_sys_key = 'sfc'),
    proposals as (select * from {{ ref('sales_proposal') }} where src_sys_key = 'sfc'),
    por_practice_areas as (select * from {{ source('portal', 'practice_areas') }} where _fivetran_deleted = false),
    por_practices as (select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false),
    por_locations as (select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false),
    por_regions as (
        select u.ukg_id as ukg_regional_manager_id, r.* 
        from {{ source('portal', 'location_regions') }} r
        left join {{ source('portal', 'users') }} as u on r.regional_manager_user_id = u.id
        where r._fivetran_deleted = false
    ),
    por_entities as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    por_states as (select * from {{ source('portal', 'states') }} where _fivetran_deleted = false),
    por_countries as (select * from {{ source('portal', 'countries') }} where _fivetran_deleted = false),
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
    int_employees as (select * from {{ ref('employee') }} where src_sys_key = 'int')
    

select
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    tasks.key,
    int_project.key as key_project,
    owners.link as key_owner,
    phase_codes.link as key_phase_code,
    phase_codes.key as key_milestone,
    por_entities.record_id as key_entity,
    sf_project.account_id as key_account,
    project_manager.key as key_project_manager,
    regional_manager.key as key_regional_manager,
    por_regions.record_id as key_region,
    por_locations.record_id as key_location,
    por_states. record_id as key_state,
    por_countries.record_id as key_country,
    por_practices.record_id as key_practice,
    por_practice_areas.record_id as key_practice_area,
    tasks.key_proposal,
    tasks.key_change_event_notification_item,
    tasks.key_project as sfc_project_id,
    tasks.owner_id as sfc_owner_id,
    tasks.amt_po_awarded,
    tasks.bln_closed_for_time_entry,
    tasks.bln_is_clone,
    tasks.bln_is_scope_change,
    tasks.bln_is_started,
    tasks.budget_original,
    tasks.cost_variance,
    tasks.currency_iso_code,
    tasks.description,
    tasks.dte_milestone_date,
    tasks.dte_planned_completion_date,
    tasks.dts_end,
    tasks.dts_src_created,
    tasks.dts_src_modified,
    tasks.dts_start,
    tasks.dts_start_actual,
    tasks.dts_system_modstamp,
    tasks.dts_update_from_cx,
    tasks.hours_estimated,
    tasks.hours_estimated_rollup,
    tasks.hours_original,
    tasks.hours_actual,
    tasks.hours_timecard_actual,
    tasks.hrs_or_units_additional,
    tasks.last_modified_by_id,
    tasks.metadata_1,
    tasks.metadata_2,
    tasks.metadata_3,
    tasks.metadata_4,
    tasks.milestone_status,
    tasks.milestone_status_indirect_tvl,
    tasks.name,
    tasks.percent_complete_tasks,
    tasks.rate_original,
    tasks.rate_additional,
    tasks.status,
    tasks.system_deliverable,
    tasks.type,
    int_project.project_name as project_name,
    int_project.project_id as project_id,
    owners.display_name as owner_name,
    owners.display_name_lf as owner_name_lf,
    owners.email_address_work as owner_email,
    phase_codes.name as phase_code_name,
    por_entities.display_name as entity_name,
    sf_accts.name as account_name,
    regional_manager.display_name as regional_manager_name,
    regional_manager.display_name_lf as regional_manager_name_lf,
    regional_manager.email_address_work as regional_manager_email,
    por_regions.display_name as region_name,
    por_locations.display_name as location_name,
    por_states.display_name as state_name,
    por_countries.display_name as country_name,
    por_practices.display_name as practice_name,
    por_practice_areas.display_name as practice_area_name,
    proposals.name as proposal_name,
    null as change_event_notification_item_name
from tasks
left join project as sf_project on tasks.hash_key_project = sf_project.hash_key and sf_project.src_sys_key = 'sfc'
left join project as int_project on sf_project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'
left join phase_codes on tasks.key_phase_code = phase_codes.key
left join all_employees as owners on tasks.owner_id = owners.sfc_user_id
left join sf_accts on sf_project.account_id = sf_accts.key
left join proposals on tasks.key_proposal = proposals.key
left join por_entities on int_project.key_entity = por_entities.id
left join int_employees as int_pm on int_project.project_manager_id = int_pm.intacct_employee_id
left join all_employees as project_manager on int_pm.hash_link = project_manager.hash_link and project_manager.src_sys_key = 'ukg'
left join por_countries on sf_accts.billing_country_code = por_countries.salesforce_id
left join por_states on sf_accts.billing_state_code = por_states.salesforce_id and por_states.country_id = por_countries.id
left join por_locations on sf_accts.key_location = por_locations.salesforce_id
left join por_regions on por_locations.region_id = por_regions.id
left join all_employees as regional_manager on por_regions.ukg_regional_manager_id = regional_manager.key
left join por_practice_areas on phase_codes.key_practice_area = por_practice_areas.salesforce_id
left join por_practices on por_practice_areas.practice_id = por_practices.id