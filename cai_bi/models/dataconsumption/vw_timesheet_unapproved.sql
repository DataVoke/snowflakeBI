{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_timesheet_unapproved"
    )
}}

with
    gold_employee as (select * from {{ ref('dim_employee') }}),
    silver_employee as (select * from {{ ref('employee') }}),
    sf_timesheet as (select * from  {{ ref('timesheet') }} where src_sys_key = 'sfc'),
    sf_timesheet_entry as (select * from  {{ ref('timesheet_entry') }} where src_sys_key = 'sfc'),
    por_regions as (select * from {{ source('portal','location_regions' )}} where _fivetran_deleted = false)

select 
            sfc.key as key_timesheet,
            ukg_employee.key as key_employee,
            ifnull(ukg_employee.display_name, sfc.employee_name) as employee_name,
            ifnull(ukg_employee.display_name_lf, sfc.employee_name) as employee_name_lf,
            ukg_employee.email_address_work as employee_email,
            sfc.dte_src_start as timesheet_start_date,
            sfc.dte_src_end as timesheet_end_date,
            sum(sf_timesheet_entry.qty) as qty,
            sfc.status as timesheet_status,
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
        from sf_timesheet sfc
        left join silver_employee sfc_employee on sfc.key_employee = sfc_employee.key and sfc_employee.src_sys_key = 'sfc'
        left join gold_employee ukg_employee on sfc_employee.link = ukg_employee.key
        left join gold_employee supervisor on ukg_employee.key_supervisor = supervisor.key
        left join por_regions on ukg_employee.key_region = por_regions.record_id
        left join silver_employee por_employee on por_regions.regional_manager_user_id = por_employee.key and por_employee.src_sys_key = 'por'
        left join gold_employee regional_manager on por_employee.link = regional_manager.key
        left join sf_timesheet_entry on sfc.key = sf_timesheet_entry.key_timesheet
        where sfc.status != 'Approved'
        group by all