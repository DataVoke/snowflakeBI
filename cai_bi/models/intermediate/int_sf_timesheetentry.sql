{{
    config(
        materialized="table",
        schema="intermediate"
    )
}}

with task_time       as (select * from {{ source('salesforce', 'pse_task_time_c') }} where _fivetran_deleted = false),
     timecard_header as (select * from {{ source('salesforce', 'pse_timecard_header_c') }} where _fivetran_deleted = false),

monday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_monday_notes_c as notes,
        tt.pse_monday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_monday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
tuesday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_tuesday_notes_c as notes,
        tt.pse_tuesday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_tuesday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
wednesday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_wednesday_notes_c as notes,
        tt.pse_wednesday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_wednesday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
thursday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_thursday_notes_c as notes,
        tt.pse_thursday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_thursday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
friday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_friday_notes_c as notes,
        tt.pse_friday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_friday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
saturday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_saturday_notes_c as notes,
        tt.pse_saturday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_saturday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
sunday as (
    select 
        th.pse_resource_c,
        th.pse_project_c,
        tt.created_by_id,
        tt.last_modified_by_id,
        tt.id,
        tt.pse_timecard_c,
        tt.pse_start_date_c as task_start_date,
        tt.created_date,
        th.pse_end_date_c,
        tt.last_modified_date,
        th.pse_start_date_c as header_start_date,
        tt.pse_sunday_notes_c as notes,
        tt.pse_sunday_hours_c as qty,
        tt.pse_project_task_c,
        tt.name
    from task_time tt
    left join timecard_header th 
        on tt.pse_timecard_c = th.id
    where nullif(tt.pse_sunday_hours_c, 0) <> 0 
        and tt.is_deleted = false 
        and th.is_deleted = false
),
final as (
    select * from monday
    union all
    select * from tuesday
    union all
    select * from wednesday
    union all
    select * from thursday
    union all
    select * from friday
    union all
    select * from saturday
    union all
    select * from sunday
)

select * from final
