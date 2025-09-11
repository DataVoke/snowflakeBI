{{
    config(
        alias="labor_budgeting",
        materialized="table",
        schema="dataconsumption"
    )
}}

with
    gold_employees as (
        select * from {{ ref('dim_employee') }}
    ),
    portal_user_forecast as (
        select *, por_user.currency_id as forecast_currency_id, por_user.employee_type as portal_employee_type
        from {{ source('portal','users_forecasts') }} por_forecast
        left join {{ source('portal','users') }} por_user on por_forecast.user_id = por_user.id
        where por_forecast.timeframe_id = year(current_timestamp)
    ),
    portal_entites as (
        select e.*, eb.total as entity_benefit_rate
        from {{ source('portal','entities') }} as e
        left join {{ source('portal','entities_benefit_rates') }} as eb on e.id = eb.entity_id and eb.timeframe_id = year(current_timestamp)
    ),
    portal_departments as (
        select * from {{ source('portal','departments') }}
    ),
    currencies_active as (
        select * from {{ ref("currencies_active") }}
    ),
    fx_rates_timeseries as (
        select * from {{ ref("ref_fx_rates_timeseries") }} 
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
    current_timestamp as dts_created_at,
    'activity_by_project' as created_by,
    current_timestamp as dts_updated_at,
    'activity_by_project' as updated_by,
    gold_emp.key,
    gold_emp.portal_id,
    gold_emp.intacct_employee_id as employee_id,
    gold_emp.last_name,
    gold_emp.display_name,
    gold_emp.display_name_lf,
    gold_emp.employee_type_name,
    gold_emp.key_entity,
    gold_emp.entity_name,
    gold_emp.department_name,
    gold_emp.key_department,
    por_departments.intacct_id as department_id,
    gold_emp.base_team_name,
    gold_emp.key_base_team,
    gold_emp.pay_type_name,
    gold_emp.key_pay_type,
    gold_emp.labor_category_name,
    gold_emp.key_labor_categories,  
    ifnull(gold_emp.other_rate_2,0) as performance_bonus_percent,
    ifnull(gold_emp.cola_percent,0) as cola_percent,
    
    -- Original Employee  Values
    upper(gold_emp.currency_code) as currency_id_original, 
    1 as salary_conversion_rate_original,
    ifnull(gold_emp.hourly_pay_rate,0) as hourly_rate_original,
    ifnull(gold_emp.annual_salary,0) as annual_salary_original,
    ifnull(gold_emp.other_rate_1,0) as non_discretionary_original,
    ifnull(annual_salary_original * performance_bonus_percent,0) as performance_bonus_original,
    ifnull(annual_salary_original * cola_percent,0) as annual_cola_original,
    ifnull(gold_emp.other_rate_3,0) as other_rate_3_original,
    ifnull(gold_emp.other_rate_4,0) as other_rate_4_original,
    ifnull(forecast_cc_fx_org_to_employee.fx_rate_mul,1) as forecast_conversion_rate_original,
    ifnull(por_forecast.bill_rate,0) as bill_rate_original,
    ifnull(por_forecast.plan_bill_amount_year,0) * forecast_conversion_rate_original as plan_bill_amount_year_original,
    

    -- USD Employee Values
    'USD' as currency_id_usd, 
    ifnull(gold_cc_fx_org_to_usd.fx_rate_mul,active_currencies.default_fx_rate_to_usd) as salary_conversion_rate_usd,
    ifnull(gold_emp.hourly_pay_rate,0) * salary_conversion_rate_usd as hourly_rate_usd,
    ifnull(gold_emp.annual_salary,0) * salary_conversion_rate_usd as annual_salary_usd,
    ifnull(gold_emp.other_rate_1,0) * salary_conversion_rate_usd as non_discretionary_usd,
    ifnull(annual_salary_usd * performance_bonus_percent,0) as performance_bonus_usd,
    ifnull(annual_salary_usd * cola_percent,0) as annual_cola_usd,
    ifnull(gold_emp.other_rate_3,0) * salary_conversion_rate_usd as other_rate_3_usd,
    ifnull(gold_emp.other_rate_4,0) * salary_conversion_rate_usd as other_rate_4_usd,
    ifnull(forecast_cc_fx_org_to_usd.fx_rate_mul,1) as forecast_conversion_rate_usd,
    ifnull(por_forecast.bill_rate,0) * forecast_conversion_rate_usd as bill_rate_usd,
    ifnull(por_forecast.plan_bill_amount_year,0) * forecast_conversion_rate_usd as plan_bill_amount_year_usd,

    -- Entity Employee Values
    upper(por_entities.currency_id) as currency_id_entity,
    ifnull(gold_cc_fx_org_to_entity.fx_rate_mul,1) as salary_conversion_rate_entity,
    ifnull(gold_emp.hourly_pay_rate,0) * salary_conversion_rate_entity as hourly_rate_entity,
    ifnull(gold_emp.annual_salary,0) * salary_conversion_rate_entity as annual_salary_entity,
    ifnull(gold_emp.other_rate_1,0) * salary_conversion_rate_entity as non_discretionary_entity,
    ifnull(annual_salary_entity * performance_bonus_percent,0) as performance_bonus_entity,
    ifnull(annual_salary_entity * cola_percent,0) as annual_cola_entity,
    ifnull(gold_emp.other_rate_3,0) * salary_conversion_rate_entity as other_rate_3_entity,
    ifnull(gold_emp.other_rate_4,0) * salary_conversion_rate_entity as other_rate_4_entity,
    ifnull(forecast_cc_fx_org_to_entity.fx_rate_mul,1) as forecast_conversion_rate_entity,
    ifnull(por_forecast.bill_rate,0) * forecast_conversion_rate_entity as bill_rate_entity,
    ifnull(por_forecast.plan_bill_amount_year,0) * forecast_conversion_rate_entity as plan_bill_amount_year_entity,

    //Forecast information
    upper(por_forecast.forecast_currency_id) as currency_id_forecast,
    ifnull(por_forecast.plan_hours_year,0) as plan_hours_year,
    ifnull(por_entities.entity_benefit_rate,0) as benefit_rate,
    gold_emp.status as employee_status,
    por_forecast.portal_employee_type as employee_type    
from gold_employees gold_emp
left join portal_entites por_entities on gold_emp.key_entity = por_entities.record_id
left join portal_departments por_departments on gold_emp.key_department = por_departments.record_id
left join portal_user_forecast por_forecast on gold_emp.portal_id = por_forecast.user_id 
left join currencies_active as active_currencies on gold_emp.currency_code = active_currencies.currency
left join currency_conversion as gold_cc_fx_org_to_usd on (
                                                                upper(gold_emp.currency_code) = gold_cc_fx_org_to_usd.frm_curr 
                                                                and gold_cc_fx_org_to_usd.to_curr = 'USD'
                                                                and gold_cc_fx_org_to_usd.date = TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE())-1, 'YYYY-MM-DD')  
                                                                
                                                            )  
left join currency_conversion as gold_cc_fx_org_to_entity on (
                                                                upper(gold_emp.currency_code) = gold_cc_fx_org_to_entity.frm_curr 
                                                                and upper(por_entities.currency_id) = gold_cc_fx_org_to_entity.to_curr
                                                                and gold_cc_fx_org_to_entity.date = TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE())-1, 'YYYY-MM-DD') 
                                                            )
left join currency_conversion as forecast_cc_fx_org_to_employee on (
                                                                upper(por_forecast.forecast_currency_id) = forecast_cc_fx_org_to_employee.frm_curr 
                                                                and upper(gold_emp.currency_code) = forecast_cc_fx_org_to_employee.to_curr
                                                                and forecast_cc_fx_org_to_employee.date = TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE())-1, 'YYYY-MM-DD') 
                                                            )
left join currency_conversion as forecast_cc_fx_org_to_usd on (
                                                                upper(por_forecast.forecast_currency_id) = forecast_cc_fx_org_to_usd.frm_curr 
                                                                and forecast_cc_fx_org_to_usd.to_curr = 'USD'
                                                                and forecast_cc_fx_org_to_usd.date = TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE())-1, 'YYYY-MM-DD') 
                                                            )
left join currency_conversion as forecast_cc_fx_org_to_entity on (
                                                                upper(por_forecast.forecast_currency_id) = forecast_cc_fx_org_to_entity.frm_curr 
                                                                and upper(por_entities.currency_id) = forecast_cc_fx_org_to_entity.to_curr
                                                                and forecast_cc_fx_org_to_entity.date = TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE())-1, 'YYYY-MM-DD') 
                                                            )