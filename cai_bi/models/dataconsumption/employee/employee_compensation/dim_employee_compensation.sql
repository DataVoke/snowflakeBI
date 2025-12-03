{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="employee_compensation"
    )
}}

with 
    compensation as (select * from {{ ref('employee_compensation') }} where src_sys_key='ukg'),
    job_history as (
        select * 
        from {{ ref('employee_job_history') }} 
        where src_sys_key='ukg' and bln_is_rate_change = true
        qualify row_number() over (
            partition by key_employee_company, dte_job_effective
            order by dte_job_effective desc, annual_salary desc
        ) = 1),
    employee_ukg as (select * from {{ ref('employee') }} where src_sys_key = 'ukg'),
    int_employee as (select * from {{ ref('employee') }} where src_sys_key='int'),
    por_entities as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    por_positions as (select * from {{ source('portal', 'positions') }} where _fivetran_deleted = false),
    por_shift_codes as (select * from {{ source('portal', 'shift_codes') }} where _fivetran_deleted = false),
    por_salary_grades as (select * from {{ source('portal', 'job_salary_grades') }} where _fivetran_deleted = false),
    ukg_companies as (select * from {{ source('ukg_pro', 'company') }} where _fivetran_deleted=false),
    benefits_rate as (select * from {{ source('portal', 'entities_benefit_rates') }} where _fivetran_deleted = false),
    currencies_active as (
        select * from {{ ref('currencies_active') }}
    ),
    fx_rates_timeseries as (
        select * from {{ ref('ref_fx_rates_timeseries') }} order by date desc
    ),
    
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_mul
        from fx_rates_timeseries as cc
        where frm_curr in (select currency from currencies_active)
        and to_curr in (select currency from currencies_active)
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    compensation.key,
    compensation.key_company,
    compensation.key_employee_company,
    compensation.key_employee,
    job_history.key as key_job_history,
    ifnull(entities_int.record_id,entities_company.record_id) as key_entity,
    por_positions.record_id as key_position,
    por_shift_codes.record_id as key_shift_code,
    por_salary_grades.record_id as key_salary_grade,
    compensation.distribution_center_code,
    compensation.employee_status_code,
    compensation.job_change_reason_code,
    compensation.job_salary_grade as ukg_job_salary_grade,
    compensation.primary_job_id as ukg_job_id,
    compensation.primary_shift_code as ukg_shift_code,
    compensation.primary_shift_group_code,
    employee.email_address_work as employee_email,
    employee.display_name as employee_name,
    employee.display_name_lf as employee_name_lf,
    upper(compensation.currency_code) as currency_code_original,
    upper(ifnull(entities_int.currency_id,entities_company.currency_id)) as currency_code_entity,
    cast(ifnull(por_shift_codes.shift_percent, 0) as number(38,5)) as cola_percent,
    cast(ifnull(cc_to_entity.fx_rate_mul, 1) as number(38,10)) as conversion_rate_entity, -- Should only be null if the conversion currencies match
    cast(ifnull(cc_to_usd.fx_rate_mul, 1) as number(38,10)) as conversion_rate_usd, -- Should only be null if the conversion currencies match or the effective date
    cast(ifnull(compensation.annual_salary,0) as number(38,2)) as annual_salary_original,
    cast(annual_salary_original * conversion_rate_entity as number(38,2)) as annual_salary_entity,
    cast(annual_salary_original * conversion_rate_usd as number(38,2)) as annual_salary_usd,
    cast(annual_salary_original * (1+cola_percent) as number(38,2)) as annual_salary_cola_original,
    cast(annual_salary_entity * (1+cola_percent) as number(38,2)) as annual_salary_cola_entity,
    cast(annual_salary_usd * (1+cola_percent) as number(38,2)) as annual_salary_cola_usd,
    cast(ifnull(benefits_rate.total,0) as number(38,5)) as benefits_rate,
    compensation.bln_is_auto_paid,
    compensation.bln_is_full_time,
    compensation.bln_is_highly_compensated,
    compensation.bln_is_salary,
    compensation.company_code,
    companies.name as company_name,
    cast(compensation.dte_in_job as date) as dte_in_job,
    cast(compensation.dte_last_paid as date) as dte_last_paid,
    cast(compensation.dte_last_worked as date) as dte_last_worked,
    cast(compensation.dte_next_salary_review as date) as dte_next_salary_review,
    cast(compensation.dte_paid_thru as date) as dte_paid_thru,
    cast(compensation.dts_last_synced as timestamp_ltz) as dts_last_synced,
    ifnull(entities_int.display_name,entities_company.display_name) as entity_name,
    cast(ifnull(compensation.hourly_pay_rate,0) as number(38,2)) as hourly_pay_rate_original,
    cast(hourly_pay_rate_original * conversion_rate_entity as number(38,2)) as hourly_pay_rate_entity,
    cast(hourly_pay_rate_original * conversion_rate_usd as number(38,2)) as hourly_pay_rate_usd,
    cast(hourly_pay_rate_original * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_original,
    cast(hourly_pay_rate_entity * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_entity,
    cast(hourly_pay_rate_usd * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_usd,
    ifnull(nullif(compensation.job_title,''),por_positions.display_name) as job_title,
    cast(ifnull(compensation.number_of_payments,0) as number(38,0)) as number_of_payments,
    case 
        when ifnull(compensation.other_rate_1,0) < 10 -- This is old data that noone knows what it is. I dont want it to skew the metrics for non descrationary bonuses 
            then 0 
            else cast(ifnull(compensation.other_rate_1, 0) as number(38,2)) 
        end  as other_rate_1_original,
    cast(ifnull(other_rate_1_original * conversion_rate_entity, 0) as number(38,2)) as other_rate_1_entity,
    cast(ifnull(other_rate_1_original * conversion_rate_usd, 0) as number(38,2)) as other_rate_1_usd,
    case when ifnull(compensation.other_rate_2, 0) < 1 then cast(ifnull(compensation.other_rate_2, 0) as number(4,2)) else 0 end as other_rate_2_original,
    cast(ifnull(compensation.other_rate_3, 0) as number(38,2)) as other_rate_3_original,
    cast(other_rate_3_original * conversion_rate_entity as number(38,2)) as other_rate_3_entity,
    cast(other_rate_3_original * conversion_rate_usd as number(38,2)) as other_rate_3_usd,
    cast(ifnull(compensation.other_rate_4, 0) as number(38,2)) as other_rate_4_original,
    cast(other_rate_4_original * conversion_rate_entity as number(38,2)) as other_rate_4_entity,
    cast(other_rate_4_original * conversion_rate_usd as number(38,2)) as other_rate_4_usd,
    cast(annual_salary_original * other_rate_2_original as number(38,2)) as performance_bonus_original,
    cast(annual_salary_entity * other_rate_2_original as number(38,2)) as performance_bonus_entity,
    cast(annual_salary_usd * other_rate_2_original as number(38,2)) as performance_bonus_usd,
    compensation.pay_period,
    cast(ifnull(compensation.pay_period_pay_rate,0) as number(38,2)) as period_pay_rate_original,
    cast(period_pay_rate_original * conversion_rate_entity as number(38,2)) as period_pay_rate_entity,
    cast(period_pay_rate_original * conversion_rate_usd as number(38,2)) as period_pay_rate_usd,
    cast(period_pay_rate_original * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_original,
    cast(period_pay_rate_entity * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_entity,
    cast(period_pay_rate_usd * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_usd,
    compensation.performance_review_rating,
    compensation.performance_review_type,
    cast(ifnull(compensation.scheduled_annual_work_hours,0) as number(38,2)) as scheduled_annual_work_hours,
    cast(ifnull(compensation.scheduled_full_time_equivalency,0) as number(38,2)) as scheduled_full_time_equivalency,
    cast(ifnull(compensation.scheduled_period_work_hours,0) as number(38,2)) as scheduled_period_work_hours,
    cast(ifnull(compensation.weekly_hours,0) as number(38,2)) as weekly_hours,
    cast(ifnull(compensation.weekly_pay_rate,0) as number(38,2)) as weekly_pay_rate_original,
    cast(weekly_pay_rate_original * conversion_rate_entity as number(38,2)) as weekly_pay_rate_entity,
    cast(weekly_pay_rate_original * conversion_rate_usd as number(38,2)) as weekly_pay_rate_usd,
    cast(weekly_pay_rate_original * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_original,
    cast(weekly_pay_rate_entity * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_entity,
    cast(weekly_pay_rate_usd * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_usd,
    cast(annual_salary_cola_original + performance_bonus_original + other_rate_1_original as number(38,2)) as total_potential_compensation_original,
    cast(annual_salary_cola_usd + performance_bonus_usd + other_rate_1_usd as number(38,2)) as total_potential_compensation_usd,
    cast(annual_salary_cola_entity + performance_bonus_entity + other_rate_1_entity as number(38,2)) as total_potential_compensation_entity,
    cast(annual_salary_cola_original + other_rate_1_original as number(38,2)) as total_compensation_original,
    cast(annual_salary_cola_usd + other_rate_1_usd as number(38,2)) as total_compensation_usd,
    cast(annual_salary_cola_entity + other_rate_1_entity as number(38,2)) as total_compensation_entity,
    por_positions.display_name as position_name,
    por_shift_codes.display_name as shift_code_name,
    por_salary_grades.display_name as salary_grade_name
from compensation
left join job_history on compensation.key_employee_company = job_history.key_employee_company and coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) = job_history.dte_job_effective
left join employee_ukg as employee on compensation.key_employee = employee.key
left join por_positions on compensation.primary_job_id = por_positions.ukg_id
left join int_employee on compensation.key_employee = int_employee.link
left join por_entities as entities_int on int_employee.key_entity = entities_int.id
left join por_entities as entities_company on compensation.key_company = entities_company.ukg_id
left join benefits_rate on year(coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date())) = benefits_rate.timeframe_id and ifnull(entities_int.id,entities_company.id) = benefits_rate.entity_id
left join por_shift_codes on compensation.primary_shift_code = por_shift_codes.ukg_id
left join por_salary_grades on compensation.job_salary_grade = por_salary_grades.ukg_id
left join ukg_companies as companies on compensation.key_company = companies.id
left join currency_conversion as cc_to_usd on (
                                                cc_to_usd.frm_curr = upper(currency_code_original)
                                                and cc_to_usd.to_curr = 'USD'
                                                and cc_to_usd.date = case 
                                                        when coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) >= current_date() then current_date()
                                                        when coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) <= '2016-01-02' then '2016-01-02'
                                                        else coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) 
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )  
left join currency_conversion as cc_to_entity on (
                                                cc_to_entity.frm_curr = upper(currency_code_original)
                                                and cc_to_entity.to_curr = upper(currency_code_entity) 
                                                and cc_to_entity.date = case 
                                                        when coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) >= current_date() then current_date()
                                                        when coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date()) <= '2016-01-02' then '2016-01-02'
                                                        else coalesce(compensation.dte_in_job, compensation.dte_last_worked, current_date())
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )