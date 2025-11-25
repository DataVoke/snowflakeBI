{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="employee_job_history"
    )
}}

with 
    job_history as (select * from {{ ref('employee_job_history') }}),
    int_employee as (select * from {{ ref('employee') }} where src_sys_key='int'),
    ukg_employee as (select * from {{ ref('employee') }} where src_sys_key='ukg'),
    por_entities as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    por_practices as (select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false),
    por_positions as (select * from {{ source('portal', 'positions') }} where _fivetran_deleted = false),
    por_locations as (select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false),
    por_payroll_companies as (select * from {{ source('portal', 'payroll_companies') }} where _fivetran_deleted = false),
    por_employee_types as (select * from {{ source('portal', 'employee_types') }} where _fivetran_deleted = false),
    por_locations_ukg as (select * from {{ source('portal', 'locations_ukg') }} where _fivetran_deleted = false),
    por_locations_intacct as (select * from {{ source('portal', 'locations_intacct') }} where _fivetran_deleted = false),
    por_departments as (select * from {{ source('portal', 'departments') }} where _fivetran_deleted = false),
    por_base_teams as (select * from {{ source('portal', 'base_teams') }} where _fivetran_deleted = false),
    por_shift_codes as (select * from {{ source('portal', 'shift_codes') }} where _fivetran_deleted = false),
    por_salary_grades as (select * from {{ source('portal', 'job_salary_grades') }} where _fivetran_deleted = false),
    ukg_companies as (select * from {{ source('ukg_pro', 'company') }} where _fivetran_deleted=false),
    benefits_rate as (select * from {{ source('portal', 'entities_benefit_rates') }} where _fivetran_deleted = false),
    
    currencies_active as (
        select * from {{ ref('currencies_active') }}
    ),
    fx_rates_timeseries as (
        select * from {{ ref("ref_fx_rates_timeseries") }}  order by date desc
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
                    md5(concat(job_history.key_employee_company,':',job_history.dte_job_effective,':',job_history.dts_src_created)) as key,
                    job_history.key_company,
                    job_history.key_employee,
                    job_history.key_employee_company,
                    por_base_teams.record_id as key_base_team,
                    por_departments.record_id as key_department,
                    por_employee_types.record_id as key_employee_type,
                    coalesce(entities_ukg_location.record_id, entities_intacct_ref.record_id, entities_company.record_id) as key_entity,
                    por_payroll_companies.record_id as key_payroll_company,
                    por_locations_intacct.record_id as key_location_intacct,
                    por_locations_ukg.record_id as key_location_ukg,
                    por_practices.record_id as key_practice,    
                    positions.record_id as key_position,
                    job_history.key as key_supervisor,
                    job_history.key_practice as ukg_practice_id,
                    job_history.key_base_team as ukg_base_team_id,
                    job_history.key_department as ukg_department_id,
                    job_history.key_intacct_location as ukg_intacct_location_id,
                    por_salary_grades.record_id as key_salary_grade,
                    por_shift_codes.record_id as key_shift_code,
                    cast(ifnull(por_shift_codes.shift_percent, 0) as number(38,5)) as cola_percent,
                    cast(ifnull(cc_to_entity.fx_rate_mul, 1) as number(38,10)) as conversion_rate_entity, -- Should only be null if the conversion currencies match
                    cast(ifnull(cc_to_usd.fx_rate_mul, 1) as number(38,10)) as conversion_rate_usd, -- Should only be null if the conversion currencies match or the effective date
                    job_history.src_created_by_id,
                    nullif(job_history.employee_status_code,'') as employee_status_code,
                    nullif(job_history.employee_type_id,'') as ukg_employee_type_id,
                    nullif(job_history.home_company_id,'') as home_company_id,
                    nullif(job_history.job_id,'') as ukg_job_id,
                    ifnull(job_history.job_title, positions.display_name) as job_title,
                    positions.display_name as position_name,
                    nullif(job_history.location_id,'') as ukg_location_id,
                    nullif(job_history.pay_group_id,'') as ukg_pay_group_id,
                    cast(ifnull(job_history.annual_salary, 0) as number(38,2)) as annual_salary_original,
                    cast(ifnull(annual_salary_original * conversion_rate_entity, 0) as number(38,2)) as annual_salary_entity,
                    cast(ifnull(annual_salary_original * conversion_rate_usd, 0) as number(38,2)) as annual_salary_usd,
                    cast(ifnull(annual_salary_original * (1+cola_percent), 0) as number(38,2)) as annual_salary_cola_original,
                    cast(ifnull(annual_salary_entity * (1+cola_percent), 0) as number(38,2)) as annual_salary_cola_entity,
                    cast(ifnull(annual_salary_usd * (1+cola_percent), 0) as number(38,2)) as annual_salary_cola_usd,
                    por_base_teams.display_name as base_team_name,
                    ifnull(benefits_rate.total,0) as benefits_rate,
                    ifnull(job_history.bln_is_full_time,false) as bln_is_full_time,
                    ifnull(job_history.bln_is_job_change,false) as bln_is_job_change,
                    ifnull(job_history.bln_is_org_change,false) as bln_is_org_change,
                    ifnull(job_history.bln_is_outside_guidelines,false) as bln_is_outside_guidelines,
                    ifnull(job_history.bln_is_outside_range,false) as bln_is_outside_range,
                    ifnull(job_history.bln_is_promotion,false) as bln_is_promotion,
                    ifnull(job_history.bln_is_rate_change,false) as bln_is_rate_change,
                    ifnull(job_history.bln_is_salary,false) as bln_is_salary,
                    ifnull(job_history.bln_is_system,false) as bln_is_system,
                    ifnull(job_history.bln_is_transfer,false) as bln_is_transfer,
                    ifnull(job_history.bln_is_viewable_by_employee,false) as bln_is_viewable_by_employee,
                    ifnull(job_history.bln_supervisor_not_in_list,false) as bln_supervisor_not_in_list,
                    ifnull(job_history.bln_use_pay_scales,false) as bln_use_pay_scales,
                    coalesce(entities_company.display_name,ukg_companies.name) as company_name,
                    job_history.company_code,
                    upper(job_history.currency_code) as currency_code_original,
                    upper(coalesce(entities_ukg_location.currency_id, entities_intacct_ref.currency_id, entities_company.currency_id)) as currency_code_entity,
                    por_departments.display_name as department_name,
                    job_history.dte_integration_effective,
                    job_history.dte_job_effective,
                    job_history.dts_last_synced as dts_last_synced,
                    job_history.dts_src_created,
                    employee.email_address_work as employee_email,
                    employee.display_name as employee_name,
                    employee.display_name_lf as employee_name_lf,
                    coalesce(entities_ukg_location.display_name, entities_intacct_ref.display_name, entities_company.display_name) as entity_name,
                    nullif(job_history.flsa_category_id,'') as flsa_category_id,
                    cast(ifnull(job_history.hourly_pay_rate, 0) as number(38,2)) as hourly_pay_rate_original,
                    cast(hourly_pay_rate_original * conversion_rate_entity as number(38,2)) as hourly_pay_rate_entity,
                    cast(hourly_pay_rate_original * conversion_rate_usd as number(38,2)) as hourly_pay_rate_usd,
                    cast(hourly_pay_rate_original * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_original,
                    cast(hourly_pay_rate_entity * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_entity,
                    cast(hourly_pay_rate_usd * (1+cola_percent) as number(38,2)) as hourly_pay_rate_cola_usd,
                    nullif(job_history.notes,'') as notes,
                    cast(ifnull(job_history.number_of_payments,0) as number(38,0)) as number_of_payments,
                    case 
                        when ifnull(job_history.other_rate_1,0) < 10 -- This is old data that noone knows what it is. I dont want it to skew the metrics for non descrationary bonuses 
                            then 0 
                            else cast(ifnull(job_history.other_rate_1, 0) as number(38,2)) 
                        end  as other_rate_1_original,
                    cast(ifnull(other_rate_1_original * conversion_rate_entity, 0) as number(38,2)) as other_rate_1_entity,
                    cast(ifnull(other_rate_1_original * conversion_rate_usd, 0) as number(38,2)) as other_rate_1_usd,
                    case when ifnull(job_history.other_rate_2, 0) < 1 then cast(ifnull(job_history.other_rate_2, 0) as number(4,2)) else 0 end as other_rate_2_original,
                    cast(ifnull(job_history.other_rate_3, 0) as number(38,2)) as other_rate_3_original,
                    cast(other_rate_3_original * conversion_rate_entity as number(38,2)) as other_rate_3_entity,
                    cast(other_rate_3_original * conversion_rate_usd as number(38,2)) as other_rate_3_usd,
                    cast(ifnull(job_history.other_rate_4, 0) as number(38,2)) as other_rate_4_original,
                    cast(other_rate_4_original * conversion_rate_entity as number(38,2)) as other_rate_4_entity,
                    cast(other_rate_4_original * conversion_rate_usd as number(38,2)) as other_rate_4_usd,
                    nullif(job_history.pay_period_code,'') as pay_period_code,
                    cast(ifnull(job_history.percent_change,0) as number(38,5)) as percent_change,
                    cast(annual_salary_original * other_rate_2_original as number(38,2)) as performance_bonus_original,
                    cast(annual_salary_entity * other_rate_2_original as number(38,2)) as performance_bonus_entity,
                    cast(annual_salary_usd * other_rate_2_original as number(38,2)) as performance_bonus_usd,
                    cast(ifnull(period_pay_rate,0) as number(38,2)) as period_pay_rate_original,
                    cast(period_pay_rate_original * conversion_rate_entity as number(38,2)) as period_pay_rate_entity,
                    cast(job_history.period_pay_rate * conversion_rate_usd as number(38,2)) as period_pay_rate_usd,
                    cast(period_pay_rate_original * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_original,
                    cast(period_pay_rate_entity * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_entity,
                    cast(period_pay_rate_usd * (1+cola_percent) as number(38,2)) as period_pay_rate_cola_usd,
                    cast(ifnull(job_history.piece_pay_rate,0) as number(38,2)) as piece_pay_rate,
                    por_practices.display_name as practice_name,
                    nullif(job_history.project_code,'') as project_code,
                    nullif(job_history.reason_code,'') as reason_code,
                    nullif(job_history.salary_grade,'') as salary_grade,
                    por_salary_grades.display_name as salary_grade_name,
                    cast(ifnull(job_history.scheduled_annual_hours, 0) as number(38,2)) as scheduled_annual_hours,
                    cast(ifnull(job_history.scheduled_full_time_equivalency, 0) as number(38,2)) as scheduled_full_time_equivalency,
                    cast(ifnull(job_history.scheduled_work_hours, 0) as number(38,2)) as scheduled_work_hours,
                    nullif(job_history.shift_code,'') as shift_code,
                    nullif(job_history.shift_group_code,'') as shift_group_code,
                    por_shift_codes.display_name as shift_code_name,
                    cast(ifnull(job_history.step_number,0) as number(38,2)) as step_number,
                    supervisor.email_address_work as supervisor_email,
                    ifnull(supervisor.display_name, initcap(concat(job_history.supervisor_name_first,' ', job_history.supervisor_name_last))) as supervisor_name,
                    ifnull(supervisor.display_name_lf, initcap(concat(job_history.supervisor_name_last,', ', job_history.supervisor_name_first))) supervisor_name_lf,
                    por_locations_intacct.display_name as intacct_location_name,
                    por_employee_types.display_name as employee_type_name,
                    por_payroll_companies.display_name as payroll_company_name,
                    por_locations_ukg.display_name as ukg_location_name,
                    cast(ifnull(job_history.weekly_hours,0) as number(38,2)) as weekly_hours,
                    cast(ifnull(job_history.weekly_pay_rate,0) as number(38,2)) as weekly_pay_rate_original,
                    cast(weekly_pay_rate_original * conversion_rate_entity as number(38,2)) as weekly_pay_rate_entity,
                    cast(weekly_pay_rate_original * conversion_rate_usd as number(38,2)) as weekly_pay_rate_usd,
                    cast(weekly_pay_rate_original * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_original,
                    cast(weekly_pay_rate_entity * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_entity,
                    cast(weekly_pay_rate_usd * (1+cola_percent) as number(38,2)) as weekly_pay_rate_cola_usd,
                    cast(annual_salary_cola_original + performance_bonus_original + other_rate_1_original as number(38,2)) as total_potential_compensation_original,
                    cast(annual_salary_cola_entity + performance_bonus_entity + other_rate_1_entity as number(38,2)) as total_potential_compensation_entity,
                    cast(annual_salary_cola_usd + performance_bonus_usd + other_rate_1_usd as number(38,2)) as total_potential_compensation_usd,
                    cast(annual_salary_cola_original + other_rate_1_original as number(38,2)) as total_compensation_original,
                    cast(annual_salary_cola_entity + other_rate_1_entity as number(38,2)) as total_compensation_entity,
                    cast(annual_salary_cola_usd + other_rate_1_usd as number(38,2)) as total_compensation_usd,
from job_history
--************************************************************************************************
--Get entity joins
left join por_locations_intacct on job_history.key_intacct_location = por_locations_intacct.ukg_id
left join por_entities as entities_ukg_location on por_locations_intacct.entity_id = entities_ukg_location.id
left join int_employee on job_history.key_employee = int_employee.link
left join por_locations_intacct as int_link_location on int_employee.location_id_intacct = int_link_location.intacct_id
left join por_entities as entities_intacct_ref on int_link_location.entity_id = entities_intacct_ref.id
left join por_entities as entities_company on job_history.key_company = entities_company.ukg_id
--************************************************************************************************
left join ukg_companies on job_history.key_company = ukg_companies.id
left join por_practices on job_history.key_practice = por_practices.ukg_id
left join por_departments on job_history.key_department = por_departments.ukg_id
left join por_base_teams on job_history.key_base_team = por_base_teams.ukg_id
left join ukg_employee as employee on job_history.key_employee = employee.key
left join ukg_employee as supervisor on job_history.key_supervisor = supervisor.key
left join por_positions as positions on job_history.job_id = positions.ukg_id
left join por_payroll_companies on job_history.pay_group_id = por_payroll_companies.ukg_id
left join por_employee_types on job_history.employee_type_id = por_employee_types.ukg_id
left join por_locations_ukg on job_history.location_id = por_locations_ukg.ukg_id
left join benefits_rate on year(job_history.dte_job_effective) = benefits_rate.timeframe_id and coalesce(entities_ukg_location.id, entities_intacct_ref.id, entities_company.id) = benefits_rate.entity_id
left join por_shift_codes on job_history.shift_code = por_shift_codes.ukg_id
left join por_salary_grades on job_history.salary_grade = por_salary_grades.ukg_id
left join currency_conversion as cc_to_usd on (
                                                cc_to_usd.frm_curr = upper(currency_code_original)
                                                and cc_to_usd.to_curr = 'USD'
                                                and cc_to_usd.date = case 
                                                        when job_history.dte_job_effective >= current_date() then current_date()
                                                        when job_history.dte_job_effective <= '2016-01-02' then '2016-01-02'
                                                        else job_history.dte_job_effective 
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )  
left join currency_conversion as cc_to_entity on (
                                                cc_to_entity.frm_curr = upper(currency_code_original)
                                                and cc_to_entity.to_curr = upper(currency_code_entity) 
                                                and cc_to_entity.date = case 
                                                        when job_history.dte_job_effective >= current_date() then current_date()
                                                        when job_history.dte_job_effective <= '2016-01-02' then '2016-01-02'
                                                        else job_history.dte_job_effective 
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )