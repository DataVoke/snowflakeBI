{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="employee_payroll_deductions"
    )
}}
with 
    deduction           as (select key, key_employee, key_company, check_id, deduction_code, pay_group_id, payee_id, tax_calculation_group_id, timeclock_code, 
    wage_garnishment_wage_code, amt_benefit, amt_current_arrears, amt_custom_1, amt_custom_2, amt_deduction_calculation_basis, amt_employee_current, amt_employee_goal, 
    amt_employee_goal_to_date, amt_employee_original, amt_employer_current, amt_wage_garnishment_additional_arrearage, amt_wage_garnishment_disposable_income, 
    amt_wage_garnishment_exempt, amt_wage_garnishment_exempt_from_levy, amt_wage_garnishment_exemt_from_garn, amt_wage_garnishment_subject_to_cs, 
    amt_wage_garnishment_subject_to_garnishment, amt_year_to_date_deferred_compensation_combined, benefit_option, benefit_provider, bln_allow_partial_deduction, 
    bln_deduction_for_bonus_only, bln_exclude_from_workers_compensation, bln_is_401_k, bln_is_403_b, bln_is_408_k, bln_is_408_p, bln_is_457, bln_is_457_b, 
    bln_is_457_f, bln_is_501_c, bln_is_d_125, bln_is_deduction_off_set, bln_is_deferred_compensation, bln_is_dependent_care, bln_is_housing, bln_is_non_qualified_plan, 
    bln_is_prorated, bln_is_public_employee_retirement, bln_is_section_125, bln_is_voided, bln_is_voiding_record, wage_garnishment_minimum_wage_factor, child_support_type, 
    deduction_plan_type, deduction_type, deferred_compensation_cap, deferred_compensation_regular_cap, dte_custom, dte_employee_deduction_last_paid, dte_pay, employee_calculaiton_rule, 
    employee_calculation_rate_or_percent, employee_goal_rule, employee_number, employer_annual_cap_calculation_rule, employer_calculation_rate_or_percent, employer_calculation_rule, 
    employer_period_cap_calculation_rule, general_ledger_expense_account, general_ledger_pay_account, period_control, tax_category, vendor, bln_wage_garnishment_allocate_as_arrears, 
    wage_garnishment_deduction_tax_category
        from {{ ref('employee_payroll_deductions') }}
    ),
    employee            as (select key, currency_code, display_name, display_name_lf, email_address_work, link, intacct_override_entity_id, intacct_location_key, src_sys_key 
                            from {{ ref('employee') }}
    ),
    entities            as (select record_id, id, name, display_name, currency_id 
                            from {{ source('portal', 'entities') }}
                            where _fivetran_deleted = false
    ),
    companies           as (select id, name 
                            from {{ source('ukg_pro', 'company') }} 
                            where _fivetran_deleted = false
    ),
    sageint_locations   as (select recordno, parentkey 
                            from {{ source('sage_intacct','location') }} 
                            where _fivetran_deleted = false
    ),
    currency_conversion as (select frm_curr, to_curr, date, fx_rate_mul
                            from {{ ref('ref_fx_rates_timeseries') }} as cc
                            where frm_curr in (select currency from {{ ref('currencies_active') }})
                            and to_curr in (select currency from {{ ref('currencies_active') }})
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    deduction.key,
    deduction.key_company,
    deduction.key_employee,
    ifnull(override_entity.record_id, entities.record_id) as key_entity,
    deduction.check_id,
    deduction.deduction_code,
    deduction.pay_group_id,
    deduction.payee_id,
    deduction.tax_calculation_group_id,
    deduction.timeclock_code,
    deduction.wage_garnishment_wage_code,
    deduction.amt_benefit,
    deduction.amt_current_arrears,
    deduction.amt_custom_1,
    deduction.amt_custom_2,
    deduction.amt_deduction_calculation_basis,
    deduction.amt_employee_current,
    deduction.amt_employee_goal,
    deduction.amt_employee_goal_to_date,
    deduction.amt_employee_original,
    deduction.amt_employer_current,
    deduction.amt_wage_garnishment_additional_arrearage,
    deduction.amt_wage_garnishment_disposable_income,
    deduction.amt_wage_garnishment_exempt,
    deduction.amt_wage_garnishment_exempt_from_levy,
    deduction.amt_wage_garnishment_exemt_from_garn,
    deduction.amt_wage_garnishment_subject_to_cs,
    deduction.amt_wage_garnishment_subject_to_garnishment,
    deduction.amt_year_to_date_deferred_compensation_combined,
    deduction.benefit_option,
    deduction.benefit_provider,
    deduction.bln_allow_partial_deduction,
    deduction.bln_deduction_for_bonus_only,
    deduction.bln_exclude_from_workers_compensation,
    deduction.bln_is_401_k,
    deduction.bln_is_403_b,
    deduction.bln_is_408_k,
    deduction.bln_is_408_p,
    deduction.bln_is_457,
    deduction.bln_is_457_b,
    deduction.bln_is_457_f,
    deduction.bln_is_501_c,
    deduction.bln_is_d_125,
    deduction.bln_is_deduction_off_set,
    deduction.bln_is_deferred_compensation,
    deduction.bln_is_dependent_care,
    deduction.bln_is_housing,
    deduction.bln_is_non_qualified_plan,
    deduction.bln_is_prorated,
    deduction.bln_is_public_employee_retirement,
    deduction.bln_is_section_125,
    deduction.bln_is_voided,
    deduction.bln_is_voiding_record,
    deduction.wage_garnishment_minimum_wage_factor,
    deduction.child_support_type,
    deduction.deduction_plan_type,
    deduction.deduction_type,
    deduction.deferred_compensation_cap,
    deduction.deferred_compensation_regular_cap,
    deduction.dte_custom,
    deduction.dte_employee_deduction_last_paid,
    deduction.dte_pay,
    deduction.employee_calculaiton_rule,
    deduction.employee_calculation_rate_or_percent,
    deduction.employee_goal_rule,
    deduction.employee_number,
    deduction.employer_annual_cap_calculation_rule,
    deduction.employer_calculation_rate_or_percent,
    deduction.employer_calculation_rule,
    deduction.employer_period_cap_calculation_rule,
    deduction.general_ledger_expense_account,
    deduction.general_ledger_pay_account,
    deduction.period_control,
    deduction.tax_category,
    deduction.vendor,
    deduction.bln_wage_garnishment_allocate_as_arrears,
    deduction.wage_garnishment_deduction_tax_category,
    companies.name as company_name,
    ifnull(por.intacct_override_entity_id,entities.display_name) as entity_id,
    ifnull(override_entity.name, entities.name) as entity_name,
    ifnull(override_entity.currency_id, entities.currency_id) as currency_iso_code_entity,
    upper(ukg.currency_code) as currency_iso_code,
    ukg.display_name as employee_name,
    ukg.display_name_lf as employee_name_lf,
    ukg.email_address_work as employee_email,
    ifnull(cc_to_usd.fx_rate_mul,1) as conversion_rate_usd,
    ifnull(cc_to_entity.fx_rate_mul,1) as conversion_rate_entity,
    cast(deduction.amt_benefit * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_benefit_usd,
    cast(deduction.amt_current_arrears * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_current_arrears_usd,
    cast(deduction.amt_custom_1 * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_custom_1_usd,
    cast(deduction.amt_custom_2 * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_custom_2_usd,
    cast(deduction.amt_deduction_calculation_basis * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_deduction_calculation_basis_usd,
    cast(deduction.amt_employee_current * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_employee_current_usd,
    cast(deduction.amt_employee_goal * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_employee_goal_usd,
    cast(deduction.amt_employee_goal_to_date * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_employee_goal_to_date_usd,
    cast(deduction.amt_employee_original * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_employee_original_usd,
    cast(deduction.amt_employer_current * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_employer_current_usd,
    cast(deduction.amt_wage_garnishment_additional_arrearage * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_additional_arrearage_usd,
    cast(deduction.amt_wage_garnishment_disposable_income * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_disposable_income_usd,
    cast(deduction.amt_wage_garnishment_exempt * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exempt_usd,
    cast(deduction.amt_wage_garnishment_exempt_from_levy * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exempt_from_levy_usd,
    cast(deduction.amt_wage_garnishment_exemt_from_garn * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exemt_from_garn_usd,
    cast(deduction.amt_wage_garnishment_subject_to_cs * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_subject_to_cs_usd,
    cast(deduction.amt_wage_garnishment_subject_to_garnishment * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_subject_to_garnishment_usd,
    cast(deduction.amt_year_to_date_deferred_compensation_combined * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_year_to_date_deferred_compensation_combined_usd,
    cast(deduction.amt_benefit * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_benefit_entity,
    cast(deduction.amt_current_arrears * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_current_arrears_entity,
    cast(deduction.amt_custom_1 * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_custom_1_entity,
    cast(deduction.amt_custom_2 * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_custom_2_entity,
    cast(deduction.amt_deduction_calculation_basis * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_deduction_calculation_basis_entity,
    cast(deduction.amt_employee_current * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_employee_current_entity,
    cast(deduction.amt_employee_goal * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_employee_goal_entity,
    cast(deduction.amt_employee_goal_to_date * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_employee_goal_to_date_entity,
    cast(deduction.amt_employee_original * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_employee_original_entity,
    cast(deduction.amt_employer_current * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_employer_current_entity,
    cast(deduction.amt_wage_garnishment_additional_arrearage * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_additional_arrearage_entity,
    cast(deduction.amt_wage_garnishment_disposable_income * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_disposable_income_entity,
    cast(deduction.amt_wage_garnishment_exempt * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exempt_entity,
    cast(deduction.amt_wage_garnishment_exempt_from_levy * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exempt_from_levy_entity,
    cast(deduction.amt_wage_garnishment_exemt_from_garn * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_exemt_from_garn_entity,
    cast(deduction.amt_wage_garnishment_subject_to_cs * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_subject_to_cs_entity,
    cast(deduction.amt_wage_garnishment_subject_to_garnishment * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_wage_garnishment_subject_to_garnishment_entity,
    cast(deduction.amt_year_to_date_deferred_compensation_combined * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_year_to_date_deferred_compensation_combined_entity,
from deduction
left join employee ukg on deduction.key_employee = ukg.link and ukg.src_sys_key = 'ukg'
left join employee sin on deduction.key_employee = sin.link and sin.src_sys_key = 'int'
left join employee por on deduction.key_employee = por.link and por.src_sys_key = 'por'
left join sageint_locations on sin.intacct_location_key = sageint_locations.recordno
left join entities entities on ifnull(sageint_locations.parentkey, sageint_locations.recordno) = entities.id
left join entities override_entity on por.intacct_override_entity_id = override_entity.display_name
left join companies on (deduction.key_company = companies.id)
left join currency_conversion as cc_to_usd on (
                                                cc_to_usd.frm_curr = upper(ukg.currency_code)
                                                and cc_to_usd.to_curr = 'USD'
                                                and cc_to_usd.date = case 
                                                        when deduction.dte_pay >= current_date() then current_date()
                                                        when deduction.dte_pay <= '2016-01-02' then '2016-01-02'
                                                        else deduction.dte_pay
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )  
left join currency_conversion as cc_to_entity on (
                                                cc_to_entity.frm_curr = upper(ukg.currency_code)
                                                and cc_to_entity.to_curr = ifnull(override_entity.currency_id, entities.currency_id)
                                                and cc_to_entity.date = case 
                                                        when deduction.dte_pay >= current_date() then current_date()
                                                        when deduction.dte_pay <= '2016-01-02' then '2016-01-02'
                                                        else deduction.dte_pay
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )