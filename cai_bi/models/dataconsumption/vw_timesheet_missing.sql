{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_timesheet_missing"
    )
}}

with
    gold_employee as (select * from  {{ ref('dim_employee') }}),
    gold_timesheet as (select * from {{ ref('fct_timesheet') }}),
    gold_timesheet_entry as (select * from {{ ref('fct_timesheet_entry') }}),
    silver_employee as (select * from {{ ref('employee') }}),
    timesheet_dates as (select * from {{ ref("date_groups") }} where date_group_type_id = 'W' and dte_end >= '2025-01-01' and dte_start <= current_date()),
    por_regions as (select * from {{ source('portal','location_regions') }} where _fivetran_deleted = false)

select 
            t.key as key_timesheet,
            ukg_employee.key as key_employee,
            ukg_employee.display_name as employee_name,
            ukg_employee.display_name_lf as employee_name_lf,
            ukg_employee.email_address_work as employee_email,
            d.dte_start as timesheet_start_date,
            d.dte_end as timesheet_end_date,
            ifnull(sum(gold_timesheet_entry.qty),0) as qty,
            t.status,
            ukg_employee.intacct_employee_id as intacct_id,
            ukg_employee.key_supervisor,
            supervisor.display_name as supervisor_name,
            supervisor.display_name_lf as supervisor_name_lf,
            supervisor.email_address_work as supervisor_email,
            por_regions.regional_manager_user_id,
            regional_manager.display_name as regional_manager_name,
            regional_manager.display_name_lf as regional_manager_name_lf,
            regional_manager.email_address_work as regional_manager_email,
            ukg_employee.key_entity,
            ukg_employee.entity_name,
            ukg_employee.key_region,
            ukg_employee.region_name,
            ukg_employee.key_location,
            ukg_employee.location_name,
            ukg_employee.key_department,
            ukg_employee.department_name,
            ukg_employee.key_employee_type,
            ukg_employee.employee_type_name, 
            ukg_employee.dte_src_start as dte_original_hire_date,
            to_date(ukg_employee.dts_last_hire) as dte_last_hire_date, 
            ukg_employee.dte_src_end as dte_termination_date
        from timesheet_dates d
        left join gold_employee ukg_employee on d.dte_end > to_date(ukg_employee.dts_last_hire) and (d.dte_end < ukg_employee.dte_src_end or ukg_employee.dte_src_end is null)
        left join gold_timesheet t on ukg_employee.key = t.key_employee and d.dte_start = t.dte_src_start
        left join gold_employee supervisor on ukg_employee.key_supervisor = supervisor.key
        left join por_regions on ukg_employee.key_region = por_regions.record_id
        left join silver_employee por_employee on por_regions.regional_manager_user_id = por_employee.key and por_employee.src_sys_key = 'por'
        left join gold_employee regional_manager on por_employee.link = regional_manager.key
        left join gold_timesheet_entry on t.key = gold_timesheet_entry.key_timesheet
        where lower(ukg_employee.key_employee_type) not in (1814354,1814355,474084,1815351,1833005,1833006,1143543) and t.status is null
        group by all

 