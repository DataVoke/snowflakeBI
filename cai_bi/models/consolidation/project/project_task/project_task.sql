{{
    config(
        materialized="table",
        schema="consolidation",
        alias="project_task"
    )
}}

with 
    task as (select * from {{ source('salesforce', 'pse_project_task_c') }} where _fivetran_deleted = false)
    
select
    'sfc' as src_sys_key,
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    cast(current_timestamp as timestamp_tz) as dts_eff_start,
    cast('9999-12-31' as timestamp_tz ) as dts_eff_end,
    true as bln_current,
    task.id as key,
    md5(task.id) as hash_key,
    task.id as link,
    md5(task.id) as hash_link,
    task.change_event_notification_item_c as key_change_event_notification_item,
    md5(task.change_event_notification_item_c) as hash_key_change_event_notification_item,
    task.proposal_c as key_proposal,
    md5(task.proposal_c) as hash_key_proposal,
    task.pse_milestone_c as key_phase_code,
    md5(task.pse_milestone_c) as hash_key_phase_code,
    task.pse_project_c as key_project,
    md5(task.pse_project_c) as hash_key_project,
    task.cx_deliverable_link_c as cx_deliverable_link_id,
    task.owner_id as owner_id,
    task.pse_external_task_id_c as external_task_id,
    task.created_by_id as src_created_by_id,
    task.last_modified_by_id as src_modified_by_id,
    cast(task.po_amount_awarded_c as number(35,17)) as amt_po_awarded,
    task.pse_closed_for_time_entry_c as bln_closed_for_time_entry,
    task.is_clone_c as bln_is_clone,
    task.is_scope_change_c as bln_is_scope_change,
    task.pse_started_c as bln_is_started,
    cast(task.original_proposal_budget_c as number(35,17)) as budget_original,
    cast(task.cost_variance_c as number(35,17)) as cost_variance,
    task.currency_iso_code as currency_iso_code,
    task.pse_description_c as description,
    task.milestone_date_c as dte_milestone_date,
    task.planned_completion_date_c as dte_planned_completion_date,
    task.pse_end_date_time_c as dts_end,
    task.created_date as dts_src_created,
    task.last_modified_date as dts_src_modified,
    task.pse_start_date_time_c as dts_start,
    task.pse_actual_start_date_time_c as dts_start_actual,
    task.system_modstamp as dts_system_modstamp,
    task.update_from_cx_c as dts_update_from_cx,
    cast(task.pse_estimated_hours_c as number(35,17)) as hours_estimated,
    cast(task.pse_estimated_hours_rollup_c as number(35,17)) as hours_estimated_rollup,
    cast(task.original_proposal_hrs_or_units_c as number(35,17)) as hours_original,
    cast(task.pse_actual_hours_c as number(35,17)) as hours_actual,
    cast(task.pse_timecard_actual_hours_c as number(35,17)) as hours_timecard_actual,
    cast(task.additional_hrs_or_units_c as number(35,17)) as hrs_or_units_additional,
    task.last_modified_by_id as last_modified_by_id,
    task.metadata_1_c as metadata_1,
    task.metadata_2_c as metadata_2,
    task.metadata_3_c as metadata_3,
    task.metadata_4_c as metadata_4,
    task.milestone_status_c as milestone_status,
    cast(task.milestone_status_indirect_tvl_c as number(35,17)) as milestone_status_indirect_tvl,
    task.name as name,
    cast(task.pse_percent_complete_tasks_c as number(35,17)) as percent_complete_tasks,
    cast(case 
        when ifnull(task.original_proposal_hrs_or_units_c, 0) = 0 then 0
        else ifnull(task.original_proposal_budget_c,0) / ifnull(task.original_proposal_hrs_or_units_c, 0)
    end as number(35,17)) as rate_original,
    cast(task.additional_rate_c as number(35,17)) as rate_additional,
    task.pse_status_c as status,
    task.system_deliverable_c as system_deliverable,
    task.type_c as type
from task