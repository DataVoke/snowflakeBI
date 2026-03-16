{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="employee_earning"
    )
}}

with 
    earning             as (select key, hash_key, link, hash_link, key_company, hash_key_company, key_employee, hash_key_employee, key_employment, hash_key_employment, earning_id, job_id, location_id, pay_group_id, tax_calculation_group_id, tax_category_id, time_clock_code_id, accrual_code, amt_base, amt_current, amt_job_premium, amt_ytd, amt_ytd_shift, bln_include_in_deferred_compensation, bln_include_in_deferred_compensation_hours, bln_is_voided, bln_is_voiding_record, bln_use_deduction_off_set, calculation_rule, calculation_sequence, dte_pay, gen_number, gl_follow_base_account_allocation, gross_up, gross_up_target, gross_up_tax_calculation_method, hours_current, job_premium_rate_or_percent, number_of_days, number_of_games, payout_rate_type, period_control, piece_count, project, rate_hourly_pay, rate_pay, rate_period_pay, rate_piece_pay, report_category, tip_credit, tip_gross_receipts, tip_type 
                            from {{ ref('employee_earning') }}
                            where src_sys_key='ukg'
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
    earning.key,
    earning.key_company,
    earning.key_employee,
    earning.key_employment,
    ifnull(override_entity.record_id, entities.record_id) as key_entity,
    earning.earning_id,
    earning.job_id,
    earning.location_id,
    earning.pay_group_id,
    earning.tax_calculation_group_id,
    earning.tax_category_id,
    earning.time_clock_code_id,
    earning.accrual_code,
    cast(earning.amt_base as number(38,2)) as amt_base,
    cast(earning.amt_current as number(38,2)) as amt_current,
    cast(earning.amt_job_premium as number(38,2)) as amt_job_premium,
    cast(earning.amt_ytd as number(38,2)) as amt_ytd,
    cast(earning.amt_ytd_shift as number(38,2)) as amt_ytd_shift,
    earning.bln_include_in_deferred_compensation,
    earning.bln_include_in_deferred_compensation_hours,
    earning.bln_is_voided,
    earning.bln_is_voiding_record,
    earning.bln_use_deduction_off_set,
    earning.calculation_rule,
    earning.calculation_sequence,
    ukg.currency_code as currency_iso_code,
    ifnull(override_entity.currency_id, entities.currency_id) as currency_iso_code_entity,
    earning.dte_pay,
    earning.gen_number,
    earning.gl_follow_base_account_allocation,
    earning.gross_up,
    cast(earning.gross_up_target as number(38,2)) as gross_up_target,
    earning.gross_up_tax_calculation_method,
    cast(earning.hours_current as number(38,2)) as hours_current,
    cast(earning.job_premium_rate_or_percent as number(38,2)) as job_premium_rate_or_percent,
    earning.number_of_days,
    earning.number_of_games,
    earning.payout_rate_type,
    earning.period_control,
    cast(earning.piece_count as number(38,2)) as piece_count,
    earning.project,
    cast(earning.rate_hourly_pay as number(38,2)) as rate_hourly_pay,
    cast(earning.rate_pay as number(38,2)) as rate_pay,
    cast(earning.rate_period_pay as number(38,2)) as rate_period_pay,
    cast(earning.rate_piece_pay as number(38,2)) as rate_piece_pay,
    earning.report_category,
    cast(earning.tip_credit as number(38,2)) as tip_credit,
    cast(earning.tip_gross_receipts as number(38,2)) as tip_gross_receipts,
    earning.tip_type,
    ukg.display_name as employee_name,
    ukg.display_name_lf as employee_name_lf,
    ukg.email_address_work as employee_email,
    companies.name as company_name,
    ifnull(por.intacct_override_entity_id,entities.display_name) as entity_id,
    ifnull(override_entity.name, entities.name) as entity_name,
    cast(earning.amt_base * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_base_usd,
    cast(earning.amt_current * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_current_usd,
    cast(earning.amt_job_premium * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_job_premium_usd,
    cast(earning.amt_ytd * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_ytd_usd,
    cast(earning.amt_ytd_shift * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as amt_ytd_shift_usd,
    cast(earning.rate_hourly_pay * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as rate_hourly_pay_usd,
    cast(earning.rate_pay * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as rate_pay_usd,
    cast(earning.rate_period_pay * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as rate_period_pay_usd,
    cast(earning.amt_base * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_base_entity,
    cast(earning.amt_current * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_current_entity,
    cast(earning.amt_job_premium * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_job_premium_entity,
    cast(earning.amt_ytd * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_ytd_entity,
    cast(earning.amt_ytd_shift * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as amt_ytd_shift_entity,
    cast(earning.rate_hourly_pay * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as rate_hourly_pay_entity,
    cast(earning.rate_pay * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as rate_pay_entity,
    cast(earning.rate_period_pay * ifnull(cc_to_entity.fx_rate_mul,1) as number(38,2)) as rate_period_pay_entity,
from earning
left join employee ukg on earning.key_employee = ukg.link and ukg.src_sys_key = 'ukg'
left join employee sin on earning.key_employee = sin.link and sin.src_sys_key = 'int'
left join employee por on earning.key_employee = por.link and por.src_sys_key = 'por'
left join sageint_locations on sin.intacct_location_key = sageint_locations.recordno
left join entities entities on ifnull(sageint_locations.parentkey, sageint_locations.recordno) = entities.id
left join entities override_entity on por.intacct_override_entity_id = override_entity.display_name
left join companies on (earning.key_company = companies.id)
left join currency_conversion as cc_to_usd on (
                                                cc_to_usd.frm_curr = upper(ukg.currency_code)
                                                and cc_to_usd.to_curr = 'USD'
                                                and cc_to_usd.date = case 
                                                        when earning.dte_pay >= current_date() then current_date()
                                                        when earning.dte_pay <= '2016-01-02' then '2016-01-02'
                                                        else earning.dte_pay
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )  
left join currency_conversion as cc_to_entity on (
                                                cc_to_entity.frm_curr = upper(ukg.currency_code)
                                                and cc_to_entity.to_curr = ifnull(override_entity.currency_id, entities.currency_id)
                                                and cc_to_entity.date = case 
                                                        when earning.dte_pay >= current_date() then current_date()
                                                        when earning.dte_pay <= '2016-01-02' then '2016-01-02'
                                                        else earning.dte_pay
                                                    end
                                                    --If the date is in the future or greater than yesterday, use yesterdays conversion data because currency conversion may not be updated
                                                    --If date is less than 2016-01-02 then use 2016-01-02. This is that earliest conversion currency date we pull into fx_rates_timeseries
                                            )