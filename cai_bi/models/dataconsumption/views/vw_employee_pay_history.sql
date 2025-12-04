{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_employee_pay_history"
    )
}}

with 
    job_history as (
        select 'Job History' as src, *
        from {{ref('dim_employee_job_history')}}
        where (percent_change!=0 or bln_is_rate_change = true or reason_code in ('101','100'))
        qualify row_number() over ( 
            partition by key_employee, dte_job_effective
            order by dts_src_created desc 
        ) = 1
        order by employee_name, dte_job_effective desc, dts_src_created desc
    ),
    compensation as (
        select 'Compensation' as src, *
        from {{ref('dim_employee_compensation')}}
        where key_employee not in (select distinct key_employee from job_history)
    ),
    employee as (
        select * from {{ref('dim_employee')}}
    )
        select *,
            case when LAG(dte_job_effective, 1, null) OVER (partition by key_employee ORDER BY key_employee,dte_job_effective asc) is null then '1900-01-01'
                else dte_job_effective
            end as date_from,
            LAG(dte_job_effective-1, 1, '9999-12-31') OVER (partition by key_employee ORDER BY key_employee,dte_job_effective desc) AS date_to
        from (
            select
                src,
                compensation.key,
                dte_in_job as dte_job_effective, 
                compensation.key_employee, 
                compensation.key_company,
                compensation.key_employee_company,
                cast(employee.key_base_team as varchar(5000)) as key_base_team,
                employee.key_department,
                compensation.key_entity,
                employee.key_employee_type,
                compensation.key_position,
                compensation.key_salary_grade,
                employee.key_payroll_company,
                employee.key_practice,
                employee.display_name as employee_name, 
                employee.display_name_lf as employee_name_lf, 
                employee.email_address_work as employee_email,
                compensation.annual_salary_original,
                compensation.annual_salary_entity,
                compensation.annual_salary_usd,
                compensation.annual_salary_cola_original,
                compensation.annual_salary_cola_entity,
                compensation.annual_salary_cola_usd,
                employee.base_team_name,
                compensation.benefits_rate,
                false as bln_is_promotion,
                compensation.bln_is_salary,
                compensation.cola_percent,
                compensation.company_name,
                compensation.company_code,
                compensation.conversion_rate_entity,
                compensation.conversion_rate_usd,
                compensation.currency_code_original,
                compensation.currency_code_entity,
                employee.department_name,
                compensation.entity_name,
                compensation.hourly_pay_rate_original,
                compensation.hourly_pay_rate_entity,
                compensation.hourly_pay_rate_usd,
                compensation.hourly_pay_rate_cola_original,
                compensation.hourly_pay_rate_cola_entity,
                compensation.hourly_pay_rate_cola_usd,
                compensation.job_title,
                compensation.position_name,
                employee.practice_name,
                employee.payroll_company_name,
                compensation.salary_grade_name,
                compensation.other_rate_1_original,
                compensation.other_rate_1_entity,
                compensation.other_rate_1_usd,
                compensation.other_rate_2_original,
                compensation.other_rate_3_original,
                compensation.other_rate_3_entity,
                compensation.other_rate_3_usd,
                compensation.other_rate_4_original,
                compensation.other_rate_4_entity,
                compensation.other_rate_4_usd,
                0 as percent_change,
                null as reason_code,
                compensation.performance_bonus_original,
                compensation.performance_bonus_entity,
                compensation.performance_bonus_usd,
                compensation.period_pay_rate_original,
                compensation.period_pay_rate_entity,
                compensation.period_pay_rate_usd,
                compensation.period_pay_rate_cola_original,
                compensation.period_pay_rate_cola_entity,
                compensation.period_pay_rate_cola_usd,
                compensation.total_compensation_original,
                compensation.total_compensation_entity,
                compensation.total_compensation_usd,
                compensation.weekly_pay_rate_original,
                compensation.weekly_pay_rate_entity,
                compensation.weekly_pay_rate_usd,
                compensation.weekly_pay_rate_cola_original,
                compensation.weekly_pay_rate_cola_entity,
                compensation.weekly_pay_rate_cola_usd,
                employee.status
            from compensation
            left join employee on compensation.key_employee = employee.key
            where annual_salary_usd >=2100 and annual_salary_usd <= 2000000 --wrong information was put in UKG, removing bad data
            union(
                select 
                    src,
                    job_history.key,
                    job_history.dte_job_effective, 
                    job_history.key_employee, 
                    job_history.key_company,
                    job_history.key_employee_company,
                    cast(job_history.key_base_team as varchar(5000)) as key_base_team,
                    job_history.key_department,
                    job_history.key_entity,
                    job_history.key_employee_type,
                    job_history.key_position,
                    job_history.key_practice,
                    job_history.key_salary_grade,
                    job_history.key_payroll_company,
                    employee.display_name as employee_name, 
                    employee.display_name_lf as employee_name_lf, 
                    employee.email_address_work as employee_email,
                    job_history.annual_salary_original,
                    job_history.annual_salary_entity,
                    job_history.annual_salary_usd,
                    job_history.annual_salary_cola_original,
                    job_history.annual_salary_cola_entity,
                    job_history.annual_salary_cola_usd,
                    job_history.base_team_name,
                    job_history.benefits_rate,
                    job_history.bln_is_promotion,
                    job_history.bln_is_salary,
                    job_history.cola_percent,
                    job_history.company_name,
                    job_history.company_code,
                    job_history.conversion_rate_entity,
                    job_history.conversion_rate_usd,
                    job_history.currency_code_original,
                    job_history.currency_code_entity,
                    job_history.department_name,
                    job_history.entity_name,
                    job_history.hourly_pay_rate_original,
                    job_history.hourly_pay_rate_entity,
                    job_history.hourly_pay_rate_usd,
                    job_history.hourly_pay_rate_cola_original,
                    job_history.hourly_pay_rate_cola_entity,
                    job_history.hourly_pay_rate_cola_usd,
                    job_history.job_title,
                    job_history.position_name,
                    job_history.practice_name,
                    job_history.payroll_company_name,
                    job_history.salary_grade_name,
                    job_history.other_rate_1_original,
                    job_history.other_rate_1_entity,
                    job_history.other_rate_1_usd,
                    job_history.other_rate_2_original,
                    job_history.other_rate_3_original,
                    job_history.other_rate_3_entity,
                    job_history.other_rate_3_usd,
                    job_history.other_rate_4_original,
                    job_history.other_rate_4_entity,
                    job_history.other_rate_4_usd,
                    job_history.percent_change,
                    job_history.reason_code,
                    job_history.performance_bonus_original,
                    job_history.performance_bonus_entity,
                    job_history.performance_bonus_usd,
                    job_history.period_pay_rate_original,
                    job_history.period_pay_rate_entity,
                    job_history.period_pay_rate_usd,
                    job_history.period_pay_rate_cola_original,
                    job_history.period_pay_rate_cola_entity,
                    job_history.period_pay_rate_cola_usd,
                    job_history.total_compensation_original,
                    job_history.total_compensation_entity,
                    job_history.total_compensation_usd,
                    job_history.weekly_pay_rate_original,
                    job_history.weekly_pay_rate_entity,
                    job_history.weekly_pay_rate_usd,
                    job_history.weekly_pay_rate_cola_original,
                    job_history.weekly_pay_rate_cola_entity,
                    job_history.weekly_pay_rate_cola_usd,
                    employee.status
                from job_history
                left join employee on job_history.key_employee = employee.key
                where annual_salary_usd >=2100 and annual_salary_usd <= 2000000 --wrong information was put in UKG, removing bad data
            )
        )
        qualify row_number() over ( 
            partition by key_employee, dte_job_effective
            order by dte_job_effective desc, src desc  
        ) = 1