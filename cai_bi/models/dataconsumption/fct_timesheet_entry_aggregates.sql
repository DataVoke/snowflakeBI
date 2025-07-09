{{
    config(
        schema="dataconsumption",
        alias="timesheet_entry_aggregates",
    )
}}

with
    date_listings           as (select * from {{ ref('date_listings') }}),
    date_groups             as (select * from {{ ref('date_groups') }}),
    date_groups_types       as (select * from {{ ref('date_groups_types') }}),
    currencies_active       as (select * from {{ ref('currencies_active') }}),
    fx_rates_timeseries     as (select * from {{ ref("ref_fx_rates_timeseries") }}),
    timesheet_entry         as (select * from {{ ref("fct_timesheet_entry") }}),
    time_type_phase_codes   as (select * from {{ ref("time_type_phase_codes") }}),
    employee                as (select * from {{ ref("dim_employee") }}),
    si_employee             as (select * from {{ source("sage_intacct", "employee") }}),
    si_location             as (select * from {{ source("sage_intacct", "location") }}),
    si_department           as (select * from {{ source("sage_intacct", "department") }}),
    portal_users_forecasts  as (select * from {{ source("portal", "users_forecasts") }}),
    portal_timeframes       as (select * from {{ source("portal", "timeframes") }}),
    portal_entities         as (select * from {{ source("portal", "entities") }}),
    portal_users            as (select * from {{ source("portal", "users") }}),
    portal_base_teams       as (select * from {{ source("portal", "base_teams") }}),
    date_listings_flattened as (
        select 
            dte, 
            date_group_id_week as date_group_id, 
            dg.date_group_type_id 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_week = dg.id
        union
        select 
            dte, 
            date_group_id_month as date_group_id, 
            dg.date_group_type_id 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_month = dg.id
        union
        select 
            dte, 
            date_group_id_quarter as date_group_id, 
            dg.date_group_type_id 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_quarter = dg.id
        union 
        select 
            dte, 
            date_group_id_year as date_group_id, 
            dg.date_group_type_id 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_year = dg.id
    ),
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_div, 
            fx_rate_mul
        from fx_rates_timeseries as cc
        where frm_curr in (select currency from currencies_active)
        and to_curr in (select currency from currencies_active)
    ),
    users_forecast as (
        select
            u.ukg_id as key_employee,
            f.user_id,
            t.start_date,
            year(t.enddate) as year,
            u.currency_id as currency,
            ifnull(f.plan_hours_week,0) as plan_hours_week_employee,
            ifnull(f.plan_hours_year,0) as plan_hours_year_employee,
            ifnull(f.bill_rate,0) as bill_rate_employee,
            ifnull(f.bill_rate,0) * ifnull(cc.fx_rate_mul, ac.default_fx_rate_to_usd) as  bill_rate_usd,
            ifnull(f.plan_bill_amount_week,0) as plan_bill_amount_week_employee,
            ifnull(f.plan_bill_amount_week,0) * ifnull(cc.fx_rate_mul, ac.default_fx_rate_to_usd) as  plan_bill_amount_week_usd,
            ifnull(f.plan_bill_amount_year,0) as plan_bill_amount_year_employee,
            ifnull(f.plan_bill_amount_year,0) * ifnull(cc.fx_rate_mul, ac.default_fx_rate_to_usd) as  plan_bill_amount_year_usd,
            cc.fx_rate_mul as fx_rate_to_usd,
            ac.default_fx_rate_to_usd as default_rate_to_usd, -- used as a backup in case we dont get a conversion rate. as of the start of the current quarter
            ifnull(cc.fx_rate_mul, ac.default_fx_rate_to_usd) as rate_to_usd
        from portal_users_forecasts f
        left join portal_timeframes t on f.timeframe_id = t.id
        left join portal_users u on f.user_id = u.record_id
        left join currencies_active ac on u.currency_id = ac.currency
        left join currency_conversion cc on (t.start_date = cc.date and u.currency_id = cc.frm_curr and cc.to_curr = 'usd')          
    ),
    base_timesheet_entry as (
        select
            te.key,
            te.project_id,
            te.location_key as key_location,
            te.key_employee,
            ifnull(intacct_locations.parentkey, intacct_locations.recordno) as key_entity,
            te.employee_id_intacct as employee_id,
            te.location_id,
            te.bln_billable,
            te.task_name,
            w.date_group_id,
            w.date_group_type_id as date_group_type_id,
            --the date of the time entry
            te.dte_entry,

            --project currency (if null use usd, confirmed with chris)
            ifnull(nullif(te.currency_iso_code,''),'usd') as currency_project,

            --get the employee currency to use in conversions
            intacct_employee.currency as currency_employee,

            --get the usd currency for debugging purposes
            'usd' as currency_usd,

            --get the hours logged (qty)
            ifnull(te.qty,0) as qty,
           
            iff(te.bln_billable = true and te.task_name != 'tvl', te.qty, 0) as qty_billable,
           
            -- the bill rate in the projects currency
            ifnull(bill_rate,0) as bill_rate,
           
            -- get bill rates from project to employee currency
            iff(
                ifnull(nullif(te.currency_iso_code,''),'usd') = intacct_employee.currency,
                ifnull(bill_rate,0),
                (cc_employee.fx_rate_mul * ifnull(bill_rate,0)) --ifnull(bill_rate,0)  need to find a solution to this
            ) as bill_rate_employee,

            -- get bill rates from project to usd
            iff(
                ifnull(nullif(te.currency_iso_code,''),'usd') = 'usd',
                ifnull(bill_rate,0),
                (ifnull(cc_usd.fx_rate_mul, ac.default_fx_rate_to_usd) * ifnull(bill_rate,0))
            ) as bill_rate_usd,

            --get the line item amount in the projects currency
            ifnull(ifnull(qty,0) * ifnull(bill_rate,0),0) as amount,

            --get the line item amount in the employee curency from the project currency
            ifnull(ifnull(qty,0) * ifnull(bill_rate_employee,0),0) as amount_employee,
           
            --get the line item amount in usd from the project currency
            ifnull(ifnull(qty,0) * ifnull(bill_rate_usd,0),0) as amount_usd,
           
            --the default conversion rate from the project currency to usd(current_quarter rate), used as a backup in case we dont get a conversion rate. as of the start of the current quarter
            ac.default_fx_rate_to_usd as fx_rate_to_usd_default,

            --issue: actual conversion rate from the project currency to usd. this is missing values and doesnt always have a rate.
            cc_usd.fx_rate_mul as fx_rate_to_usd,

            --the rate to use for converting prouect currency to usd, if there is an actual value, use that, otherwise use the default quarters rate.
            ifnull(cc_usd.fx_rate_mul, ac.default_fx_rate_to_usd) as converstion_rate_to_usd,

            --issue: the conversion rate from the project currency to the employee currency, need to look why this doesnt always have data
            cc_employee.fx_rate_mul as conversion_rate_to_employee
           
        from timesheet_entry as te
        left join si_employee as intacct_employee on te.employee_id_intacct = intacct_employee.employeeid
        left join si_location as intacct_locations on intacct_employee.locationid = intacct_locations.locationid
        left join currencies_active as ac on currency_project = ac.currency
        left join currency_conversion as cc_usd on (te.dte_entry = cc_usd.date and currency_project = cc_usd.frm_curr and cc_usd.to_curr = 'usd')      
        left join currency_conversion as cc_employee on (te.dte_entry = cc_employee.date and te.currency_iso_code = cc_employee.frm_curr and intacct_employee.currency = cc_employee.to_curr)
        left join date_listings_flattened as w on te.dte_entry = w.dte
    ),
    total_data as (  
        --********************week total*********************
        select
            'total' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(entities.work_hours_per_week,0)
                when te.date_group_type_id = 'm'
                    then ifnull(entities.work_hours_per_week,0) * 52 / 12
                when te.date_group_type_id = 'q'
                    then ifnull(entities.work_hours_per_week,0) * 52 / 4
                when te.date_group_type_id = 'y'
                    then ifnull(entities.work_hours_per_week,0) * 52
            end as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            ifnull(f.bill_rate_employee,0) as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            ifnull(f.bill_rate_usd,0) as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(f.plan_bill_amount_week_employee,0)
                when te.date_group_type_id = 'm'
                    then ifnull(f.plan_bill_amount_year_employee,0) / 12
                when te.date_group_type_id = 'q'
                    then ifnull(f.plan_bill_amount_year_employee,0) / 4
                when te.date_group_type_id = 'y'
                    then ifnull(f.plan_bill_amount_year_employee,0)
            end as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(f.plan_bill_amount_week_usd,0)
                when te.date_group_type_id = 'm'
                    then ifnull(f.plan_bill_amount_year_usd,0) / 12
                when te.date_group_type_id = 'q'
                    then ifnull(f.plan_bill_amount_year_usd,0) / 4
                when te.date_group_type_id = 'y'
                    then ifnull(f.plan_bill_amount_year_usd,0)
            end as expected_amount_usd
        from base_timesheet_entry as te
        left join users_forecast as f on te.key_employee = f.key_employee and yearofweek(te.dte_entry) = f.year
        left join portal_entities entities on te.key_entity = entities.id
        group by all
    ),
    billable_data as (
        select
            'billable' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(f.plan_hours_week_employee,0)
                when te.date_group_type_id = 'm'
                    then ifnull(f.plan_hours_year_employee,0) / 12
                when te.date_group_type_id = 'q'
                    then ifnull(f.plan_hours_year_employee,0) / 4
                when te.date_group_type_id = 'y'
                    then ifnull(f.plan_hours_year_employee,0)
            end as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            ifnull(f.bill_rate_employee,0) as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            ifnull(f.bill_rate_usd,0) as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(f.plan_bill_amount_week_employee,0)
                when te.date_group_type_id = 'm'
                    then ifnull(f.plan_bill_amount_year_employee,0) / 12
                when te.date_group_type_id = 'q'
                    then ifnull(f.plan_bill_amount_year_employee,0) / 4
                when te.date_group_type_id = 'y'
                    then ifnull(f.plan_bill_amount_year_employee,0)
            end as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            case
                when te.date_group_type_id = 'w'
                    then ifnull(f.plan_bill_amount_week_usd,0)
                when te.date_group_type_id = 'm'
                    then ifnull(f.plan_bill_amount_year_usd,0) / 12
                when te.date_group_type_id = 'q'
                    then ifnull(f.plan_bill_amount_year_usd,0) / 4
                when te.date_group_type_id = 'y'
                    then ifnull(f.plan_bill_amount_year_usd,0)
            end as expected_amount_usd
        from base_timesheet_entry as te
        left join users_forecast as f on te.key_employee = f.key_employee and yearofweek(te.dte_entry) = f.year
        where te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true
        group by all
    ),
    internal_data as (
        select
            'internal' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            0 as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            0 as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            0 as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            0 as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            0 as expected_amount_usd
        from base_timesheet_entry as te
        where (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'internaltvl'))
            or (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable=false)
            or (te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'internal') and te.bln_billable=false)
        group by all
    ),
    tvl_data as (
        select
            'tvl' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            0 as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            0 as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            0 as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            0 as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            0 as expected_amount_usd
        from base_timesheet_entry as te
        where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable = true
        group by all
    ),
    pto_data as (
        select
            'pto' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            0 as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            0 as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            0 as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            0 as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            0 as expected_amount_usd
        from base_timesheet_entry as te
        where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'pto')
        group by all
    ),
    disfun_data as (
        select
            'disfun' as type,
            te.key_employee,
            te.employee_id,
            te.date_group_id,
            te.date_group_type_id,
            sum(te.qty) as hours,
            0 as expected_hours,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_employee,
            0 as expected_rate_employee,
            iff(sum(iff(te.bill_rate>0,te.qty,0)) >0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate>0,1,0)),0) as avg_rate_usd,
            0 as expected_rate_usd,
            sum(te.amount_employee) as amount_employee,
            0 as expected_amount_employee,
            sum(te.amount_usd) as amount_usd,
            0 as expected_amount_usd
        from base_timesheet_entry as te
        where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'disfun')
        group by all
    ),
    combined as (
        select 0 as pto_hours, t.*  from total_data t
        union( select p.hours pto_hours, b.* from billable_data b left join pto_data p on b.employee_id = p.employee_id and b.date_group_id=p.date_group_id and
    b.date_group_type_id = p.date_group_type_id)
        union(select 0 as pto_hours, i.* from internal_data i)
        union(select 0 as pto_hours, t.* from tvl_data t)
        union(select 0 as pto_hours, p.* from pto_data p)
        union(select 0 as pto_hours, d.* from disfun_data d)
    )
    select pto_hours,
        concat(te.employee_id, te.date_group_id, te.type) as id,
        te.key_employee,
        te.type,
        te.date_group_id,
        dgt.id as date_group_type_id,
        dgt.name as date_group_type_name,
        dg.year,
        dg.value as date_value,
        dg.display_name as date_group_display_name,
        dg.display_name_1 as date_group_display_name_1,
        te.employee_id,
        e.ukg_employee_number,
        initcap(ifnull(nullif(e.display_name,''),intacct_employees.personalinfo_printas)) as employee_name,
        initcap(ifnull(nullif(e.display_name_lf,''),intacct_employees.contact_name)) as employee_name_lf,
        ifnull(intacct_locations.parentkey,intacct_locations.recordno) as entity_key,
        ifnull(e.entity_name, portal_entities.display_name) as entity_name,
        intacct_employees.locationid as location_id,
        initcap(ifnull(e.location_name,intacct_locations.name)) as location_name,
        intacct_employees.departmentid as department_id,
        initcap(ifnull(e.department_name, intacct_departments.title)) as department_name,
        intacct_employees.home_region as base_team_id,
        initcap(ifnull(portal_base_teams.display_name, intacct_employees.home_region)) as base_team_name,
        ifnull(e.pay_type_name, intacct_employees.earningtypename) as pay_type_name,
        ifnull(e.bln_mst, intacct_employees.mst_member) as bln_mst,
        ifnull(e.bln_is_active,false) as bln_active,
        ifnull(upper(e.currency_code),intacct_employees.currency) as currency_employee,
        'usd' as currency_usd,
        upper(intacct_employees.currency) as currency_intacct,
        te.hours,
        te.expected_hours,
        te.avg_rate_employee,
        te.expected_rate_employee,
        te.avg_rate_usd,
        te.expected_rate_usd,
        te.amount_employee,
        te.expected_amount_employee,
        te.amount_usd,
        te.expected_amount_usd
    from combined te
    left join date_groups as dg on te.date_group_id = dg.id
    left join date_groups_types as dgt on te.date_group_type_id = dgt.id
    left join employee as e on te.key_employee = e.key
    left join si_employee as intacct_employees on te.employee_id = intacct_employees.employeeid
    left join si_location as intacct_locations on intacct_employees.locationid = intacct_locations.locationid
    left join si_department as intacct_departments on intacct_employees.departmentkey = intacct_departments.recordno
    left join portal_base_teams as portal_base_teams on intacct_employees.home_region = portal_base_teams.intacct_id
    left join portal_entities as portal_entities on ifnull(intacct_locations.parentkey,intacct_locations.recordno) = portal_entities.id