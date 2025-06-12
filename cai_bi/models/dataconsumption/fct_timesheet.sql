{{ config(
    materialized = "table",
    schema = "dataconsumption",
    alias = "timesheet"
) }}

with int as (
    select *
    from {{ ref('timesheet') }}
    where src_sys_key = 'int'
),

por_dep as (
    select *
    from {{ source('portal', 'departments') }}
    where _fivetran_deleted = false
),

por_loc as (
    select *
    from {{ source('portal', 'locations') }}
    where _fivetran_deleted = false
      and id != '55-1'
),

por_ent as (
    select *
    from {{ source('portal', 'entities') }}
    where _fivetran_deleted = false
),

locations_intacct as (
    select *
    from {{ source('sage_intacct', 'location') }}
    where _fivetran_deleted = false
),

employee_int as (
    select *
    from {{ ref('employee') }}
    where src_sys_key = 'int'
),

employee_ukg as (
    select *
    from {{ ref('employee') }}
    where src_sys_key = 'ukg'
)

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    int.key,
    por_loc.record_id as key_location,
    por_dep.record_id as key_department,
    por_ent.record_id as key_entity,
    employee_ukg.key as key_employee,

    por_dep.display_name as department_name,
    por_loc.display_name as location_name,
    por_ent.display_name as entity_name,
    employee_ukg.display_name as employee_name,

    int.employee_department_id,
    int.employee_id_intacct,
    int.employee_id,
    int.employee_location_id,

    int.src_created_by_id,
    int.src_modified_by_id,
    int.sup_doc_id,
    int.sup_doc_key,
    ifnull(nullif(locations_intacct.parentid, ''), int.employee_location_id) as entity_id,
    int.bln_cost_actual,
    int.config,
    int.description,
    int.dte_gl_post,
    int.dte_src_end,
    int.dte_src_start,
    int.dts_src_created,
    int.dts_src_modified,
    int.employee_first_name,
    int.employee_last_name,
    int.hours_in_day,
    int.mega_entity_name,
    int.method,
    int.record_url,
    int.state_worked,
    int.status,
    int.uom

from int
left join por_dep on int.employee_department_id = por_dep.intacct_id
left join por_loc on por_loc.intacct_id = int.employee_location_id
left join por_ent on por_loc.entity_id = por_ent.id
left join locations_intacct on int.employee_location_id = locations_intacct.locationid
left join employee_int on int.employee_id = employee_int.intacct_employee_id and employee_int.src_sys_key = 'int'
left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link and employee_ukg.src_sys_key = 'ukg'