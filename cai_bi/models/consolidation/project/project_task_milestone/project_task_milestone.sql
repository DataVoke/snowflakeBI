{{
    config(
        materialized="table",
        schema="consolidation",
        alias="project_task_milestone"
    )
}}

with 
    milestone as (select * from {{ source('salesforce', 'project_task_milestone_c') }} where _fivetran_deleted = false)
select 
    'sfc' as src_sys_key,
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    cast(current_timestamp as timestamp_tz) as dts_eff_start,
    cast('9999-12-31' as timestamp_tz ) as dts_eff_end,
    true as bln_current,
    milestone.id as key,
    md5(milestone.id) as hash_key,
    milestone.id as link,
    md5(milestone.id) as hash_link,
    milestone.phase_code_c as key_milestone,
    md5(milestone.phase_code_c) as hash_key_milestone,
    milestone.project_task_c as key_project_task,
    md5(milestone.project_task_c) as hash_key_project_task,
    milestone.project_c as key_project,
    md5(milestone.project_c) as hash_key_project,
    milestone.created_by_id as src_created_by_id,
    milestone.last_modified_by_id as src_modified_by_id,
    milestone.name as name,
    milestone.currency_iso_code as currency_iso_code,
    milestone.created_date as dts_src_created,
    milestone.last_modified_date as dts_src_modified,
    milestone.system_modstamp as dts_system_modstamp,
    cast(milestone.earned_value_c as number(35,17)) as earned_value,
    milestone.milestone_date_c as dte_milestone,
    milestone.status_c as status,
    cast(milestone.status_indirect_tvl_c as number(35,17)) as status_indirect_tvl,
    milestone.type_c as type,
    cast(milestone.percent_completed_tasks_c as number(35,17)) as perc_complete
from milestone