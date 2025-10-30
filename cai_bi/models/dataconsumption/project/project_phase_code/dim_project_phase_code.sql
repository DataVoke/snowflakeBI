{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="project_phase_code"
    )
}}

with
    phase_codes as (select * from {{ ref('project_phase_code') }}),
    sf_accts as (select * from {{ ref('sales_account') }} where src_sys_key = 'sfc'),
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
    project as (select * from {{ ref('project') }}),
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
    int_employees as (select * from {{ ref('employee') }} where src_sys_key = 'int'),
    intacct_data as (
        select 
            cast(current_timestamp as timestamp_tz) as dts_created_at,
            '{{ this.name }}' as created_by,
            cast(current_timestamp as timestamp_tz) as dts_updated_at,
            '{{ this.name }}' as updated_by,
            phase_codes.key,
            phase_codes.key_customer,
            phase_codes.key_item,
            phase_codes.key_parent,
            phase_codes.key_project,
            approver.key as key_approver,
            project_manager.key as key_project_manager,
            por_practice_areas.record_id as key_practice_area,
            por_practices.record_id as key_practice,
            psa_phase_codes.key_product,
            m_phase_codes.key as key_milestone,
            pt_phase_codes.key as key_project_task,
            por_entities.record_id as key_entity,
            sf_project.account_id,
            regional_manager.key as key_regional_manager,
            por_regions.record_id as key_region,
            por_locations.record_id as key_location,
            por_states.record_id as key_state,
            por_countries.record_id as key_country,
            project.customer_id,
            phase_codes.int_id,
            phase_codes.item_id,
            phase_codes.parent_id,
            project.project_id,
            psa_phase_codes.pts_id,
            m_phase_codes.record_type_id,
            m_phase_codes.key_approver as sfc_approver_id,
            psa_phase_codes.key_project as pts_project_id,
            m_phase_codes.key_project as sfc_project_id,
            ifnull(m_phase_codes.amt_billable_entered,0) as amt_billable_entered,
            ifnull(m_phase_codes.amt_billable_in_financials,0) as amt_billable_in_financials,
            ifnull(m_phase_codes.amt_billable_submitted,0) as amt_billable_submitted,
            ifnull(m_phase_codes.amt_milestone,0) as amt_milestone,
            m_phase_codes.bln_closed_for_expense_entry,
            pt_phase_codes.bln_closed_for_time_entry,
            phase_codes.bln_is_billable,
            ifnull(psa_phase_codes.bln_is_custom,true) as bln_is_custom,
            true as bln_is_in_intacct,
            phase_codes.bln_is_utilized,
            psa_phase_codes.budget_calculated,
            psa_phase_codes.budget_override,
            ifnull(m_phase_codes.cost_milestone,0) as cost_milestone,
            ifnull(pt_phase_codes.cost_variance,0) as cost_variance,
            nullif(ifnull(nullif(pt_phase_codes.currency_iso_code,''),project.currency_iso_code),'') as currency_iso_code,
            project.customer_name,
            nullif(phase_codes.description,'') as description,
            phase_codes.dte_end,
            project.dte_src_end as dte_project_end,
            phase_codes.dte_start,
            project.dte_src_start as dte_start_project,
            psa_phase_codes.dts_int_last_sync,
            psa_phase_codes.dts_sfc_phase_code_last_sync,
            psa_phase_codes.dts_sfc_task_last_sync,
            phase_codes.dts_src_created,
            phase_codes.dts_src_modified,
            m_phase_codes.dts_system_modstamp,
            ifnull(pt_phase_codes.hours_actual,0) as hours_actual,
            ifnull(pt_phase_codes.hours_actual_timecard,0) as hours_actual_timecard,
            ifnull(m_phase_codes.hours_billable_entered,0) as hours_billable_entered,
            ifnull(psa_phase_codes.hours_planned,0) as hours_planned,
            phase_codes.item_name,
            phase_codes.name,
            ifnull(m_phase_codes.original_additional_budget,0) as original_additional_budget,
            ifnull(m_phase_codes.original_additional_hours,0) as original_additional_hours,
            ifnull(pt_phase_codes.original_budget,0) as original_budget,
            ifnull(pt_phase_codes.original_hours,0) as original_hours,
            phase_codes.parent_name,
            project.project_name,
            ifnull(phase_codes.qty_actual,0) as qty_actual,
            ifnull(phase_codes.qty_approved,0) as qty_approved,
            ifnull(phase_codes.qty_billable_actual,0) as qty_billable_actual,
            ifnull(phase_codes.qty_billable_approved,0) as qty_billable_approved,
            phase_codes.record_url,
            ifnull(m_phase_codes.scope_change_total_budget,0) as scope_change_total_budget,
            ifnull(m_phase_codes.scope_change_total_hours,0) as scope_change_total_hours,
            phase_codes.status,
            ifnull(m_phase_codes.task_count,0) as task_count,
            ifnull(m_phase_codes.total_earned_value,0) as total_earned_value,
            pt_phase_codes.type,
            approver.display_name as approver_name,
            approver.display_name_lf as approver_name_lf,
            approver.email_address_work as approver_email,
            project_manager.display_name as project_manager_name,
            project_manager.display_name_lf as project_manager_name_lf,
            project_manager.email_address_work as project_manager_email,
            regional_manager.display_name as regional_manager_name,
            regional_manager.display_name_lf as regional_manager_name_lf,
            regional_manager.email_address_work as regional_manager_email,
            sf_accts.name as account_name,
            case when psa_phase_codes.key_product = 1 then 'Regular Services'
                 when psa_phase_codes.key_product = 2 then 'autoLOTO Services'
                 when psa_phase_codes.key_product = 3 then 'Kneat Services'
                 when psa_phase_codes.key_product = 4 then 'Internal Services'
            end as product_name,
            por_practice_areas.display_name as practice_area_name,
            por_practices.display_name as practice_name,
            por_entities.display_name as entity_name,
            por_entities.currency_id as entity_currency,
            por_regions.display_name as region_name,
            por_locations.display_name as location_name,
            por_countries.display_name as country_name,
            por_states.display_name as state_name
        from phase_codes
        left join phase_codes m_phase_codes on phase_codes.hash_link = m_phase_codes.hash_link and m_phase_codes.src_sys_key = 'sfc_milestones'
        left join phase_codes pt_phase_codes on phase_codes.hash_link = pt_phase_codes.hash_link and pt_phase_codes.src_sys_key = 'sfc_project_task'
        left join phase_codes psa_phase_codes on phase_codes.hash_link = psa_phase_codes.hash_link and psa_phase_codes.src_sys_key = 'pts'
        left join all_employees as approver on m_phase_codes.key_approver = approver.sfc_user_id
        left join por_practice_areas on m_phase_codes.key_practice_area = por_practice_areas.salesforce_id
        left join por_practices on por_practice_areas.practice_id = por_practices.id
        left join project on phase_codes.key_project = project.key and project.src_sys_key = 'int'
        left join project as sf_project on project.hash_link = sf_project.hash_link and sf_project.src_sys_key = 'sfc'
        left join sf_accts on sf_project.account_id = sf_accts.key
        left join por_entities on project.key_entity = por_entities.id
        left join int_employees as int_pm on project.project_manager_id = int_pm.intacct_employee_id
        left join all_employees as project_manager on int_pm.hash_link = project_manager.hash_link and project_manager.src_sys_key = 'ukg'
        left join por_countries on sf_accts.billing_country_code = por_countries.salesforce_id
        left join por_states on sf_accts.billing_state_code = por_states.salesforce_id and por_states.country_id = por_countries.id
        left join por_locations on sf_accts.key_location = por_locations.salesforce_id
        left join por_regions on por_locations.region_id = por_regions.id
        left join all_employees as regional_manager on por_regions.ukg_regional_manager_id = regional_manager.key
        where phase_codes.src_sys_key = 'int'
    ),
    sf_data as (
         --Only get the sf phase codes that are in salesforce and not intacct.
            select 
                cast(current_timestamp as timestamp_tz) as dts_created_at,
                '{{ this.name }}' as created_by,
                cast(current_timestamp as timestamp_tz) as dts_updated_at,
                '{{ this.name }}' as updated_by,
                phase_codes.key,
                int_project.customer_key as key_customer,
                null as key_item,
                null as key_parent,
                int_project.key as key_project,
                approver.key as key_approver,
                project_manager.key as key_project_manager,
                por_practice_areas.record_id as key_practice_area,
                por_practices.record_id as key_practice,
                psa_phase_codes.key_product,
                m_phase_codes.key as key_milestone,
                phase_codes.key as key_project_task,
                por_entities.record_id as key_entity,
                project.account_id,
                regional_manager.key as key_regional_manager,
                por_regions.record_id as key_region,
                por_locations.record_id as key_location,
                por_states.record_id as key_state,
                por_countries.record_id as key_country,
                int_project.customer_id,
                null as int_id,
                null item_id,
                null as parent_id,
                int_project.project_id,
                psa_phase_codes.pts_id,
                m_phase_codes.record_type_id,
                m_phase_codes.key_approver as sfc_approver_id,
                psa_phase_codes.key_project as pts_project_id,
                m_phase_codes.key_project as sfc_project_id,
                m_phase_codes.amt_billable_entered,
                m_phase_codes.amt_billable_in_financials,
                m_phase_codes.amt_billable_submitted,
                m_phase_codes.amt_milestone,
                m_phase_codes.bln_closed_for_expense_entry,
                phase_codes.bln_closed_for_time_entry,
                false as bln_is_billable,
                ifnull(psa_phase_codes.bln_is_custom, true) as bln_is_custom,
                false as bln_is_in_intacct,
                false as bln_is_utilized,
                psa_phase_codes.budget_calculated,
                psa_phase_codes.budget_override,
                ifnull(m_phase_codes.cost_milestone,0) as cost_milestone,
                ifnull(phase_codes.cost_variance,0) as cost_variance,
                nullif(ifnull(nullif(phase_codes.currency_iso_code,''), project.currency_iso_code),'') as currency_iso_code,
                int_project.customer_name,
                nullif(phase_codes.description,'') as description,
                phase_codes.dte_end,
                project.dte_src_end as dte_project_end,
                phase_codes.dte_start,
                project.dte_src_start as dte_start_project,
                psa_phase_codes.dts_int_last_sync,
                psa_phase_codes.dts_sfc_phase_code_last_sync,
                psa_phase_codes.dts_sfc_task_last_sync,
                phase_codes.dts_src_created,
                phase_codes.dts_src_modified,
                m_phase_codes.dts_system_modstamp,
                ifnull(phase_codes.hours_actual,0) as hours_actual,
                ifnull(phase_codes.hours_actual_timecard,0) as hours_actual_timecard,
                ifnull(m_phase_codes.hours_billable_entered,0) as hours_billable_entered,
                ifnull(psa_phase_codes.hours_planned,0) as hours_planned,
                null as item_name,
                phase_codes.name,
                ifnull(m_phase_codes.original_additional_budget,0) as original_additional_budget,
                ifnull(m_phase_codes.original_additional_hours,0) as original_additional_hours,
                ifnull(phase_codes.original_budget,0) as original_budget,
                ifnull(phase_codes.original_hours,0) as original_hours,
                null as parent_name,
                project.project_name,
                0 as qty_actual,
                0 as qty_approved,
                0 as qty_billable_actual,
                0 as qty_billable_approved,
                null as record_url,
                ifnull(m_phase_codes.scope_change_total_budget,0) as scope_change_total_budget,
                ifnull(m_phase_codes.scope_change_total_hours,0) as scope_change_total_hours,
                case when phase_codes.status = 'Started' then 'In Progress'
                    when phase_codes.status = 'Complete' then 'Completed'
                    else phase_codes.status
                end as status,
                m_phase_codes.task_count,
                ifnull(m_phase_codes.total_earned_value,0) as total_earned_value,
                phase_codes.type,
                approver.display_name as approver_name,
                approver.display_name_lf as approver_name_lf,
                approver.email_address_work as approver_email,
                project_manager.display_name as project_manager_name,
                project_manager.display_name_lf as project_manager_name_lf,
                project_manager.email_address_work as project_manager_email,
                regional_manager.display_name as regional_manager_name,
                regional_manager.display_name_lf as regional_manager_name_lf,
                regional_manager.email_address_work as regional_manager_email,
                sf_accts.name as account_name,
                case when psa_phase_codes.key_product = 1 then 'Regular Services'
                     when psa_phase_codes.key_product = 2 then 'autoLOTO Services'
                     when psa_phase_codes.key_product = 3 then 'Kneat Services'
                     when psa_phase_codes.key_product = 4 then 'Internal Services'
                end as product_name,
                por_practice_areas.display_name as practice_area_name,
                por_practices.display_name as practice_name,
                por_entities.display_name as entity_name,
                por_entities.currency_id as entity_currency,
                por_regions.display_name as region_name,
                por_locations.display_name as location_name,
                por_countries.display_name as country_name,
                por_states.display_name as state_name
            from phase_codes 
            left join phase_codes m_phase_codes on phase_codes.key_milestone = m_phase_codes.key and m_phase_codes.src_sys_key = 'sfc_milestones'
            left join phase_codes psa_phase_codes on phase_codes.key = psa_phase_codes.sfc_task_id and psa_phase_codes.src_sys_key = 'pts'
            left join all_employees as approver on m_phase_codes.key_approver = approver.sfc_user_id
            left join por_practice_areas on m_phase_codes.key_practice_area = por_practice_areas.salesforce_id
            left join por_practices on por_practice_areas.practice_id = por_practices.id
            left join project on phase_codes.hash_key_project = project.hash_key and project.src_sys_key = 'sfc'
            left join project int_project on project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'
            left join sf_accts on project.account_id = sf_accts.key
            left join por_entities on int_project.key_entity = por_entities.id
            left join int_employees as int_pm on int_project.project_manager_id = int_pm.intacct_employee_id
            left join all_employees as project_manager on int_pm.hash_link = project_manager.hash_link and project_manager.src_sys_key = 'ukg'
            left join por_countries on sf_accts.billing_country_code = por_countries.salesforce_id
            left join por_states on sf_accts.billing_state_code = por_states.salesforce_id and por_states.country_id = por_countries.id
            left join por_locations on sf_accts.key_location = por_locations.salesforce_id
            left join por_regions on por_locations.region_id = por_regions.id
            left join all_employees as regional_manager on por_regions.ukg_regional_manager_id = regional_manager.key
            where phase_codes.src_sys_key = 'sfc_project_task' and phase_codes.link is null
    ),
    final as (
        select * from intacct_data
        union(select * from sf_data)
    )

    select * from final