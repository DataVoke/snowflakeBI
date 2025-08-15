{{
    config(
        schema="dataconsumption",
        alias="timesheet_entry_aggregates",
    )
}}

WITH

    date_listings           as (select * from {{ ref("date_listings") }}),
    date_groups             as (select * from {{ ref("date_groups") }}),
    date_groups_types       as (select * from {{ ref("date_groups_types") }}),
    dim_employee            as (select * from {{ ref("dim_employee") }}),
    currencies_active       as (select * from {{ ref("currencies_active") }}),
    fx_rates_timeseries     as (select * from {{ ref("ref_fx_rates_timeseries") }}),
    timesheet_entry         as (select * from {{ ref("fct_timesheet_entry") }}),
    time_type_phase_codes   as (select * from {{ ref("time_type_phase_codes") }}),

    portal_users_forecasts  as (select * from {{ source("portal", "users_forecasts") }}),
    portal_timeframes       as (select * from {{ source("portal", "timeframes") }}),
    portal_users            as (select * from {{ source("portal", "users") }}),
    portal_entities         as (select * from {{ source("portal", "entities") }}),
    portal_base_teams       as (select * from {{ source("portal", "base_teams") }}),

    sage_intacct_employee   as (select * from {{ source("sage_intacct", "employee") }}),
    sage_intacct_location   as (select * from {{ source("sage_intacct", "location") }}),
    sage_intacct_department as (select * from {{ source("sage_intacct", "department") }}),

    date_listings_flattened as (
        select 
            dte, 
            date_group_id_week as date_group_id, 
            dg.date_group_type_id,
            dg.year 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_week = dg.id
        union
        select 
            dte, 
            date_group_id_month as date_group_id, 
            dg.date_group_type_id,
            dg.year  
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_month = dg.id
        union
        select 
            dte, 
            date_group_id_quarter as date_group_id, 
            dg.date_group_type_id,
            dg.year 
        from date_listings as dl
        left join date_groups as dg on dl.date_group_id_quarter = dg.id
        union
        select 
            dte, 
            date_group_id_year as date_group_id, 
            dg.date_group_type_id,
            dg.year  
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
        from portal_users_forecasts as f
        left join portal_timeframes as t on f.timeframe_id = t.id
        left join portal_users as u on f.user_id = u.record_id
        left join currencies_active ac on u.currency_id = ac.currency
        left join currency_conversion cc on (t.start_date = cc.date and u.currency_id = cc.frm_curr and cc.to_curr = 'USD')          
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

            --the date group year used to join to the forecasting table
            w.year as date_group_year,

            --project currency (if null use usd, confirmed with chris)
            ifnull(nullif(te.currency_iso_code,''),'USD') as currency_project,

            --GET THE EMPLOYEE CURRENCY TO USE IN CONVERSIONS
            intacct_employee.currency as currency_employee,

            --GET THE USD CURRENCY FOR DEBUGGING PURPOSES
            'USD' as currency_usd,

            --GET THE HOURS LOGGED (QTY)
            ifnull(te.qty,0) as qty,
           
            iff(te.bln_billable = true and te.task_name != 'TVL', te.qty, 0) as qty_billable,
           
            -- THE BILL RATE IN THE PROJECTS CURRENCY
            ifnull(bill_rate,0) as bill_rate,
           
            -- GET BILL RATES FROM PROJECT TO EMPLOYEE CURRENCY
            iff(
                ifnull(nullif(te.currency_iso_code,''),'USD') = intacct_employee.currency,
                ifnull(bill_rate,0),
                (cc_employee.fx_rate_mul * ifnull(bill_rate,0)) --ifnull(bill_rate,0)  need to find a solution to this
            ) as bill_rate_employee,

            -- GET BILL RATES FROM PROJECT TO USD
            iff(
                ifnull(nullif(te.currency_iso_code,''),'USD') = 'USD',
                ifnull(bill_rate,0),
                (ifnull(cc_usd.fx_rate_mul, ac.default_fx_rate_to_usd) * ifnull(bill_rate,0))
            ) as bill_rate_usd,

            --GET THE LINE ITEM AMOUNT IN THE PROJECTS CURRENCY
            ifnull(ifnull(qty,0) * ifnull(bill_rate,0),0) as amount,

            --GET THE LINE ITEM AMOUNT IN THE EMPLOYEE CURENCY FROM THE PROJECT CURRENCY
            ifnull(ifnull(qty,0) * ifnull(bill_rate_employee,0),0) as amount_employee,
           
            --GET THE LINE ITEM AMOUNT IN USD FROM THE PROJECT CURRENCY
            ifnull(ifnull(qty,0) * ifnull(bill_rate_usd,0),0) as amount_usd,
           
            --THE DEFAULT CONVERSION RATE FROM THE PROJECT CURRENCY TO USD(CURRENT_QUARTER RATE), USED AS A BACKUP IN CASE WE DONT GET A CONVERSION RATE. AS OF THE START OF THE CURRENT QUARTER
            ac.default_fx_rate_to_usd as fx_rate_to_usd_default,

            --ISSUE: ACTUAL CONVERSION RATE FROM THE PROJECT CURRENCY TO USD. THIS IS MISSING VALUES AND DOESNT ALWAYS HAVE A RATE.
            cc_usd.fx_rate_mul as fx_rate_to_usd,

            --THE RATE TO USE FOR CONVERTING PROUECT CURRENCY TO USD, IF THERE IS AN ACTUAL VALUE, USE THAT, OTHERWISE USE THE DEFAULT QUARTERS RATE.
            ifnull(cc_usd.fx_rate_mul, ac.default_fx_rate_to_usd) as converstion_rate_to_usd,

            --ISSUE: THE CONVERSION RATE FROM THE PROJECT CURRENCY TO THE EMPLOYEE CURRENCY, NEED TO LOOK WHY THIS DOESNT ALWAYS HAVE DATA
            cc_employee.fx_rate_mul as conversion_rate_to_employee
           
        from timesheet_entry as te
        left join sage_intacct_employee as intacct_employee on te.employee_id_intacct = intacct_employee.employeeid
        left join sage_intacct_location as intacct_locations on intacct_employee.locationid = intacct_locations.locationid
        left join currencies_active as ac on currency_project = ac.currency
        left join currency_conversion as cc_usd on (te.dte_entry = cc_usd.date and currency_project = cc_usd.frm_curr and cc_usd.to_curr = 'USD')      
        left join currency_conversion as cc_employee on (te.dte_entry = cc_employee.date and te.currency_iso_code = cc_employee.frm_curr and intacct_employee.currency = cc_employee.to_curr)
        left join date_listings_flattened as w on te.dte_entry = w.dte
    ),
blank_data as (
        select
            te.key_employee, te.key_entity, te.employee_id,te.date_group_id, te.date_group_type_id, te.date_group_year,
            0 as hours,  
            0 as avg_rate_employee,
            0 as avg_rate_usd,
            0 as amount_employee,
            0 as amount_usd
        from base_timesheet_entry as te
        group by all
    ),
    total_data as (  
        --********************WEEK TOTAL*********************
        select
            'Total' as type, 1 as type_sort, te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
            sum(te.qty) as hours,
            iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
            iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
            sum(te.amount_employee) as amount_employee,
            sum(te.amount_usd) as amount_usd
        FROM base_timesheet_entry as te
        group by all
    ),
    billable_data as (
        select 'Billable' as type,2 as type_sort, key_employee, key_entity,employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours, sum(avg_rate_employee) as avg_rate_employee, sum(avg_rate_usd) as avg_rate_usd, sum(amount_employee) as amount_employee, sum(amount_usd) as amount_usd
        from (
            select
                te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,            
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
                sum(te.amount_employee) as amount_employee,
                sum(te.amount_usd) as amount_usd
            from base_timesheet_entry as te
            where te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours,  avg_rate_employee, avg_rate_usd, amount_employee, amount_usd
                from blank_data
            )
        ) as b
        group by all
    ),
    
     pto_data as (
        select 'PTO' as type,3 as type_sort, key_employee, key_entity,employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours, sum(avg_rate_employee) as avg_rate_employee, sum(avg_rate_usd) as avg_rate_usd, sum(amount_employee) as amount_employee, sum(amount_usd) as amount_usd
        from (
            select
                te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
                sum(te.amount_employee) as amount_employee,
                sum(te.amount_usd) as amount_usd,
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'pto')
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours,  avg_rate_employee, avg_rate_usd, amount_employee, amount_usd
                from blank_data
            )
        ) as p
        group by all
    ),

    internal_data as (
        select 'Internal' as type, 4 as type_sort, key_employee, key_entity,employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours, sum(avg_rate_employee) as avg_rate_employee, sum(avg_rate_usd) as avg_rate_usd, sum(amount_employee) as amount_employee, sum(amount_usd) as amount_usd
        from (
            select
                te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                iff(sum(iff(te.bill_rate > 0,te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
                iff(sum(iff(te.bill_rate > 0,te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
                sum(te.amount_employee) as amount_employee,
                sum(te.amount_usd) as amount_usd
            from base_timesheet_entry as te
            where (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'internaltvl'))
            or (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable=false)
            or (te.task_name not in (select phase_code from time_type_phase_codes where time_type in ('internal','disfun')) and te.bln_billable=false)
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours,  avg_rate_employee, avg_rate_usd, amount_employee, amount_usd
                from blank_data
            )
        ) as i
        group by all
    ),

    tvl_data as (
        select 'TVL' as type, 5 as type_sort, key_employee, key_entity,employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours, sum(avg_rate_employee) as avg_rate_employee, sum(avg_rate_usd) as avg_rate_usd, sum(amount_employee) as amount_employee, sum(amount_usd) as amount_usd
        from (
            select
                te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
                sum(te.amount_employee) as amount_employee,
                sum(te.amount_usd) as amount_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours,  avg_rate_employee, avg_rate_usd, amount_employee, amount_usd
                from blank_data
            )
        ) as t
        group by all
    ),
    
    disfun_data as (
        select 'DISFUN' as type, 6 as type_sort, key_employee, key_entity,employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours, sum(avg_rate_employee) as avg_rate_employee, sum(avg_rate_usd) as avg_rate_usd, sum(amount_employee) as amount_employee, sum(amount_usd) as amount_usd
        from (
            select
                te.key_employee, te.key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_employee) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_employee,
                iff(sum(iff(te.bill_rate > 0, te.qty, 0)) > 0 , sum(te.bill_rate_usd) / sum(iff(te.bill_rate > 0, 1, 0)), 0) as avg_rate_usd,
                sum(te.amount_employee) as amount_employee,
                sum(te.amount_usd) as amount_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'disfun')
            group by all
        union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours,  avg_rate_employee, avg_rate_usd, amount_employee, amount_usd
                from blank_data
            )
        ) as d
        group by all
    ),

    combined as (
        Select te.*,
            case
                when te.type = 'Total' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(entities.work_hours_per_week,0)
                        when te.date_group_type_id = 'M' then ifnull(entities.work_hours_per_week,0) * 52 / 12
                        when te.date_group_type_id = 'Q' then ifnull(entities.work_hours_per_week,0) * 52 / 4
                        when te.date_group_type_id = 'Y' then ifnull(entities.work_hours_per_week,0) * 52
                        else 0
                    end
                when te.type = 'Billable' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(f.plan_hours_week_employee, 0)
                        when te.date_group_type_id = 'M' then ifnull(f.plan_hours_year_employee, 0) / 12
                        when te.date_group_type_id = 'Q' then ifnull(f.plan_hours_year_employee, 0) / 4
                        when te.date_group_type_id = 'Y' then ifnull(f.plan_hours_year_employee, 0)
                        else 0
                    end
                else 0
            end  as expected_hours,
            case
                when te.type = 'Total' or te.type = 'Billable' then  ifnull(f.bill_rate_employee, 0)
                else 0
            end as expected_rate_employee,
            case
                when te.type = 'Total' or te.type = 'Billable' then  ifnull(f.bill_rate_usd, 0)
                else 0
            end as expected_rate_usd,
            case
                when te.type = 'Total' or te.type = 'Billable' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(f.plan_bill_amount_week_employee, 0)
                        when te.date_group_type_id = 'M' then ifnull(f.plan_bill_amount_year_employee, 0) / 12
                        when te.date_group_type_id = 'Q' then ifnull(f.plan_bill_amount_year_employee, 0) / 4
                        when te.date_group_type_id = 'Y' then ifnull(f.plan_bill_amount_year_employee, 0)
                        else 0
                    end    
                else 0                
            end as expected_amount_employee,
            case
                when te.type = 'Total' or te.type = 'Billable' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(f.plan_bill_amount_week_usd, 0)
                        when te.date_group_type_id = 'M' then ifnull(f.plan_bill_amount_year_usd, 0) / 12
                        when te.date_group_type_id = 'Q' then ifnull(f.plan_bill_amount_year_usd, 0) / 4
                        when te.date_group_type_id = 'Y' then ifnull(f.plan_bill_amount_year_usd, 0)
                        else 0
                    end
                else 0                
            end as expected_amount_usd
        from (
            select 0 as pto_hours, t.* from total_data t
            union 
                select p.hours pto_hours, b.* 
                from billable_data b 
                left join pto_data p on b.employee_id = p.employee_id 
                    and b.date_group_id=p.date_group_id 
                    and b.date_group_type_id = p.date_group_type_id
            union
                select 0 as pto_hours, i.* from internal_data i
            union 
                select 0 as pto_hours, t.* from tvl_data t
            union
                select 0 as pto_hours, p.* from pto_data p
            union
            select 0 as pto_hours, d.* from disfun_data d
        ) as te
        left join users_forecast f on te.hash_key_employee = f.hash_key_employee and te.date_group_year = f.year
        left join portal_entities entities on te.key_entity = entities.id
    )

    select pto_hours,
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        concat(te.employee_id, te.date_group_id, te.type) as id,
        te.key_employee,
        te.type,
        te.type_sort,
        te.date_group_id,
        dgt.id as date_group_type_id,
        dgt.name as date_group_type_name,
        dg.year,
        dg.value as date_value,
        dg.display_name as date_group_display_name,
        dg.display_name_1 as date_group_display_name_1,
        te.employee_id,
        e.ukg_employee_number,
        initcap(ifnull(nullif(e.display_name, ''), intacct_employees.personalinfo_printas)) as employee_name,
        initcap(ifnull(nullif(e.display_name_lf, ''), intacct_employees.contact_name)) as employee_name_lf,
        ifnull(intacct_locations.parentkey, intacct_locations.recordno) as entity_key,
        ifnull(e.entity_name, portal_entities.display_name) as entity_name,
        intacct_employees.locationid as location_id,
        initcap(ifnull(e.location_name, intacct_locations.name)) as location_name,
        intacct_employees.departmentid as department_id,
        initcap(ifnull(e.department_name, intacct_departments.title)) as department_name,
        intacct_employees.home_region as base_team_id,
        initcap(ifnull(portal_base_teams.display_name, intacct_employees.home_region)) as base_team_name,
        case 
            when e.employee_type_name is not null then concat(ifnull(e.pay_type_name, intacct_employees.earningtypename),' - ',e.employee_type_name)
            else ifnull(e.pay_type_name, intacct_employees.earningtypename) 
        end as pay_type_name,
        ifnull(e.bln_mst, intacct_employees.mst_member) as bln_mst,
        ifnull(e.bln_is_active,false) as bln_active,
        ifnull(upper(e.currency_code),intacct_employees.currency) as currency_employee,
        'USD' as currency_usd,
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
    left join dim_employee as e on te.hash_key_employee = e.hash_key
    left join sage_intacct_employee as intacct_employees on te.employee_id = intacct_employees.employeeid
    left join sage_intacct_location as intacct_locations on intacct_employees.locationid = intacct_locations.locationid
    left join sage_intacct_department as intacct_departments on intacct_employees.departmentkey = intacct_departments.recordno
    left join portal_base_teams as portal_base_teams on intacct_employees.home_region = portal_base_teams.intacct_id
    left join portal_entities as portal_entities on ifnull(intacct_locations.parentkey,intacct_locations.recordno) = portal_entities.id