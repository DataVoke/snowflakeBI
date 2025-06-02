{{
    config(
        materialized="table",
        schema="dataconsumption",
    )
}}

with

    int as (select * from {{ ref('timesheetentry') }}),
    sfc as (
        select 
            hash_link, 
            sum(qty) as qty, 
            listagg(notes, ', ') within group(order by notes) as notes
    from int
    where src_sys_key = 'sfc'
    group by hash_link 
    ),
    departments as (select * from {{ source('portal', 'departments') }} where _fivetran_deleted = false),
    locations as (select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false),
    locations_intacct as (select * from {{ source('sage_intacct', 'location') }} where _fivetran_deleted = false),
    entities as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    practice_areas as (select * from {{ source('portal', 'practice_areas') }} where _fivetran_deleted = false),
    project as (select * from {{ ref('project') }})

select 
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    -- keys
    int.key,
    int.key_timesheet,
    departments.record_id as key_department,
    locations.record_id as key_location,
    entities.record_id as key_entity,
    practice_areas.record_id as key_practice_area,
    project.key as key_project,
    -- --names
    departments.display_name as department_name,
    locations.display_name as location_name,
    entities.display_name as entity_name,
    practice_areas.display_name as practice_area_name,
    project.project_id as project_id,
    project.project_name as project_name,
    -- fields
    int.billu_acct_key,
    int.customer_id,
    int.department_id,
    int.department_key,
    int.employee_earning_type_key,
    int.employee_id_intacct,
    ifnull(nullif(locations_intacct.parentid,''),int.location_id) as entity_id,
    int.item_id,
    int.labor_gl_batch_key,
    int.location_id,
    int.location_key,
    int.non_billnu_acct_key,
    int.non_billu_acct_key,
    int.stat_gl_batch_key,
    int.stat_journal_key,
    int.task_id,
    int.amt_labor_gl_entry,
    int.amt_labor_glentry_trx,
    int.amt_stat_gl_entry,
    int.bill_rate,
    int.bln_billable,
    int.bln_billed,
    int.customer_name,
    int.dte_entry,
    int.dte_gl_post,
    int.dte_src_created,
    int.dte_src_end,
    int.dte_src_modified,
    int.dte_src_start,
    int.employee_name,
    int.item_name,
    int.labor_gl_entry_cost_rate,
    int.labor_gl_entry_line_no,
    int.labor_gl_entry_offset_line_no,
    int.line_no,
    ifnull(nullif(sfc.notes,''),int.notes) as notes,
    int.qty,
    sfc.qty as qty_salesforce,
    int.qty_approved,
    int.qty_approved_billable,
    int.qty_approved_non_billable,
    int.qty_approved_non_utilized,
    int.qty_approved_utilized,
    int.qty_billable,
    int.qty_non_billable,
    int.qty_non_utilized,
    int.qty_utilized,
    int.record_url,
    int.stat_gl_entry_line_no,
    int.state,
    int.key_task as task_key,
    int.key_timesheet_entry as timesheet_entry_ref,
    int.task_name
from dev_bi_dw.consolidation.timesheetentry int
left join (
    select hash_link, sum(qty) as qty, listagg(notes, ', ') within group(order by notes) as notes
    from dev_bi_dw.consolidation.timesheetentry
    where src_sys_key = 'sfc'
    group by hash_link
) sfc on int.hash_link = sfc.hash_link 
left join departments on int.department_id = departments.intacct_id
left join locations on int.location_id = locations.intacct_id and locations.id != '55-1'
left join locations_intacct on int.location_key = locations_intacct.recordno
left join entities on ifnull(locations_intacct.parentkey,int.location_key) = entities.id
left join practice_areas on int.department_id = practice_areas.intacct_id
left join project on int.key_project = project.key
where int.src_sys_key = 'int'