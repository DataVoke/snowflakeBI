{{
    config(
        materialized='table',
    )
}}

with
    employee_job_history as (select * from {{ source('ukg_pro', 'employee_job_history') }} where _fivetran_deleted = false),
    employee as (select * from {{ source('ukg_pro', 'employee') }} where _fivetran_deleted = false),
    employment as (select * from {{ source('ukg_pro', 'employment') }} where _fivetran_deleted = false),
    company as (select * from {{ source('ukg_pro', 'company') }} where _fivetran_deleted = false),
    job as (select * from {{ source('ukg_pro', 'job') }} where _fivetran_deleted = false),

ukg as (
    select
        'ukg' as src_sys_key,
        cast(current_timestamp as timestamp_tz) as dts_created_at,
        '{{ this.name }}' as created_by,
        cast(current_timestamp as timestamp_tz) as dts_updated_at,
        '{{ this.name }}' as updated_by,
        cast(employee_job_history.date_time_created as timestamp_tz) as dts_eff_start,
        coalesce(cast(dateadd(second, -1,  lag(employee_job_history.date_time_created)  over (partition by employee_job_history.EMPLOYEE_ID order by employee_job_history.date_time_created desc ) ) as timestamp_tz ),'9999-12-31') as dts_eff_end,
        true as bln_current,
        employment.id as key,
        md5(employment.id) as hash_key,
        employment.employee_id as link,
        md5(employment.employee_id) as hash_link,
        employment.company_id as key_entity,
        md5(employment.company_id) as hash_key_entity,
        employment.employee_id || employment.company_id as key_employment,
        md5(employment.employee_id || employment.company_id) as hash_key_employment,
        employee_job_history.organization_level_3_id as base_team_id,
        employee_job_history.organization_level_2_id as department_id,
        employment.full_time_or_part_time_code as dol_status_id,
        employment.employee_type_code as employee_type_id,
        employee_job_history.home_company_id,
        employee_job_history.job_id,
        employee_job_history.organization_level_4_id as location_id_intacct,
        employment.primary_work_location_id as location_id_ukg,
        employment.salary_or_hourly as pay_type_id,
        employment.pay_group as payroll_company_id,
        employment.primary_job_id as position_id,
        employee_job_history.organization_level_1_id as practice_id,
        cast(employee_job_history.created_by_user_id as string) as src_created_by_id,
        null as src_modified_by_id,
        employment.supervisor_id,
        employment.term_reason as termination_type_id,
        cast(employee_job_history.annual_salary as number(19, 4)) as annual_salary,
        case 
            when employment.employee_status_code in ('A', 'L', 'O') then true else false
        end as bln_is_active,
        company.currency_code,
        cast(employee.date_time_created as timestamp_tz) as dte_src_created,
        cast(employment.date_of_termination as date) as dte_src_end,
        cast(employee.date_time_changed as timestamp_tz) as dte_src_modified,
        cast(employment.original_hire_date as date) as dte_src_start,
        cast(employment.last_hire_date as timestamp_tz) as dts_last_hire,
        employee_job_history.employee_status as empl_status,
        cast(employee_job_history.hourly_pay_rate as number(19, 4)) as hourly_pay_rate,
        cast(employee_job_history.job_effective_date as timestamp_tz) as dts_job_effective,
        employee_job_history.salary_grade as job_salary_grade,
        coalesce(employee_job_history.job_title, job.title) as job_title,
        cast(employee_job_history.other_rate_1 as number(19, 4)) as other_rate_1,
        cast(employee_job_history.other_rate_2 as number(19, 4)) as other_rate_2,
        cast(employee_job_history.other_rate_3 as number(19, 4)) as other_rate_3,
        cast(employee_job_history.other_rate_4 as number(19, 4)) as other_rate_4,
        employee_job_history.pay_group_id as pay_group,
        cast(employee_job_history.period_pay_rate as number (19, 4)) as pay_period_pay_rate,
        employment.employee_status_code as ukg_status,
        employment.term_type,
        employment.termination_reason_description,
        cast(employee_job_history.weekly_pay_rate as number(19, 4)) as weekly_pay_rate,
    from employee_job_history
    left join employee on employee_job_history.employee_id = employee.id and employee_job_history.company_id = employee.company_id
    left join employment on employee_job_history.employee_id = employment.employee_id and employee_job_history.company_id = employment.company_id
    left join company on employee_job_history.company_id = company.id
    left join job on job.id = employee_job_history.job_id
)

select * from ukg