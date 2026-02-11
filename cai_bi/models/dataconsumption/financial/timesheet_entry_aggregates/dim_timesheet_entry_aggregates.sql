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
    currencies_active       as (select * from {{ ref("currencies_active") }}),
    fx_rates_timeseries     as (select * from {{ ref("ref_fx_rates_timeseries") }}),
    timesheet_entry         as (select * from {{ ref("dim_timesheet_entry") }}),
    time_type_phase_codes   as (select * from {{ ref("time_type_phase_codes") }}),
    portal_users_forecasts  as (select * from {{ source("portal", "users_forecasts") }}),
    portal_timeframes       as (select * from {{ source("portal", "timeframes") }}),
    portal_users            as (select * from {{ source("portal", "users") }}),
    portal_entities         as (select * from {{ source("portal", "entities") }}),
    dim_employee            as (select e.*, 
                                        ifnull(ent.work_hours_per_week,40) as work_hours_per_week, 
                                        260 as work_days_per_year,
                                        work_hours_per_week / 5 as work_hours_per_day,
                                        work_hours_per_week * 52 as work_hours_per_year,
                                        ent.currency_id as entity_currency_code
                                from {{ ref("dim_employee") }} as e
                                left join portal_entities ent on e.key_entity = ent.record_id
                            ),
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
            e.dte_src_start as employee_start_date,
            year(t.enddate) as year,
            e.display_name_lf,
            entities.record_id as key_entity,
            u.currency_id as currency_code_employee,
            e.entity_currency_code as currency_code_entity,            
            ifnull(iff(case when iff(year(employee_start_date) = year, true, false) = false 
                    then e.work_days_per_year
                    else (datediff(day, employee_start_date, dateadd(day, 1, date_from_parts(year, 12, 31))) -- Total days
                            - datediff(week, employee_start_date, dateadd(day, 1, date_from_parts(year, 12, 31))) * 2 -- Subtract 2 weekend days per full week
                            - (case when dayname(employee_start_date) = 'Sun' then 1 else 0 end) -- Adjust if start date is a Sunday
                            + (case when dayname(date_from_parts(year, 12, 31)) = 'Sat' then 1 else 0 end) -- Adjust if end date is a Saturday
                        )
            end>260, 260, case when iff(year(employee_start_date) = year, true, false) = false 
                    then e.work_days_per_year
                    else (datediff(day, employee_start_date, dateadd(day, 1, date_from_parts(year, 12, 31))) -- Total days
                            - datediff(week, employee_start_date, dateadd(day, 1, date_from_parts(year, 12, 31))) * 2 -- Subtract 2 weekend days per full week
                            - (case when dayname(employee_start_date) = 'Sun' then 1 else 0 end) -- Adjust if start date is a Sunday
                            + (case when dayname(date_from_parts(year, 12, 31)) = 'Sat' then 1 else 0 end) -- Adjust if end date is a Saturday
                        )
            end),260) as total_work_days_per_year,
            f.plan_hours_year as plan_hours_per_year_raw,
            cast(case when iff(year(employee_start_date) = year, true, false) = false or total_work_days_per_year >= 260
                then ifnull(f.plan_hours_year,0)
                else ifnull(f.plan_hours_year,0) * (total_work_days_per_year / 260)
            end as number(38,2)) as plan_hours_per_year,
            cast(ifnull(plan_hours_per_year / (total_work_days_per_year / 5),0) as number(38,2)) as plan_hours_per_week,
            cast(ifnull(f.bill_rate,0) as number(38,2)) as bill_rate_employee,
            cast(ifnull(f.bill_rate,0) * ifnull(emp_curr_to_entity_cc.fx_rate_mul,1) as number(38,2)) as  bill_rate_entity,
            cast(ifnull(f.bill_rate,0) * coalesce(emp_curr_to_usd_cc.fx_rate_mul, ac.default_fx_rate_to_usd,1) as number(38,2)) as  bill_rate_usd,
            cast(bill_rate_employee * plan_hours_per_week as number(38,2)) as plan_bill_amount_per_week_employee,
            cast(bill_rate_entity * plan_hours_per_week as number(38,2)) as plan_bill_amount_per_week_entity,
            cast(bill_rate_usd * plan_hours_per_week as number(38,2)) as plan_bill_amount_per_week_usd,
            cast(bill_rate_employee * plan_hours_per_year as number(38,2)) as plan_bill_amount_per_year_employee,
            cast(bill_rate_entity * plan_hours_per_year as number(38,2)) as plan_bill_amount_per_year_entity,
            cast(bill_rate_usd * plan_hours_per_year as number(38,2)) as plan_bill_amount_per_year_usd
        from portal_users_forecasts as f
        left join portal_timeframes as t on f.timeframe_id = t.id
        left join portal_users as u on f.user_id = u.record_id
        left join dim_employee as e on u.ukg_id = e.key
        left join currencies_active ac on u.currency_id = ac.currency
        left join currency_conversion emp_curr_to_usd_cc on (t.start_date = emp_curr_to_usd_cc.date 
                                                            and emp_curr_to_usd_cc.frm_curr = currency_code_employee
                                                            and emp_curr_to_usd_cc.to_curr = 'USD') 
        left join currency_conversion emp_curr_to_entity_cc on (t.start_date = emp_curr_to_entity_cc.date 
                                                            and emp_curr_to_entity_cc.frm_curr = currency_code_employee
                                                            and emp_curr_to_entity_cc.to_curr = currency_code_entity) 
        left join portal_entities entities on e.key_entity = entities.record_id
    ),

    -- ************************************************************************************************************************************************
    -- BASE_TIMESHEET_ENTRY: Get the base entries with currency conversion joined to the dates table so that the data groups correctly in later queries
    base_timesheet_entry as (
        select
            te.key,
            te.project_id,
            te.location_key as key_location,
            te.key_employee,
            te.key_entity as key_entity_project,
            te.key_entity_employee,
            te.employee_id_intacct as employee_id,
            te.location_id,
            te.bln_billable,
            te.task_name,
            w.date_group_id,
            w.date_group_type_id as date_group_type_id,
            te.dte_entry,
            w.year as date_group_year,
            ifnull(te.qty,0) as qty,         
            iff(te.bln_billable = true and te.task_name != 'TVL', te.qty, 0) as qty_billable,
            te.bill_rate,
            te.currency_iso_Code,
            
            -- get raw data in EMPLOYEE entity currency rates/numbers
            currency_code_employee_entity,
            cast(bill_rate_employee_entity as number(38,2)) as bill_rate_employee_entity,
            cast(employee_pay_hourly_rate_entity as number(38,2)) as cost_rate_employee_entity,
            cast(employee_pay_hourly_rate_cola_entity as number(38,2)) as cost_rate_cola_employee_entity,
            cast(bill_rate_employee_entity * qty as number(38,2)) as bill_amount_employee_entity,
            cast(cost_rate_employee_entity * qty as number(38,2)) as cost_amount_employee_entity,
            cast(cost_rate_cola_employee_entity * qty as number(38,2)) as cost_amount_cola_employee_entity,

            -- get raw data in PROJECT entity currency rates/numbers
            currency_code_project_entity,
            cast(bill_rate_employee_project_entity as number(38,2)) as bill_rate_project_entity,
            cast(employee_pay_hourly_rate_project_entity as number(38,2)) as cost_rate_project_entity,
            cast(employee_pay_hourly_rate_cola_project_entity as number(38,2)) as cost_rate_cola_project_entity,
            cast(bill_rate_project_entity * qty as number(38,2)) as bill_amount_project_entity,
            cast(cost_rate_project_entity * qty as number(38,2)) as cost_amount_project_entity,
            cast(cost_rate_cola_project_entity * qty as number(38,2)) as cost_amount_cola_project_entity,

            -- get raw data in USD currency rates/numbers
            'USD' as currency_code_employee_usd,
            cast(bill_rate_employee_usd as number(38,2)) as bill_rate_usd,
            cast(employee_pay_hourly_rate_usd as number(38,2)) as cost_rate_usd,
            cast(employee_pay_hourly_rate_cola_usd as number(38,2)) as cost_rate_cola_usd,
            cast(bill_rate_usd * qty as number(38,2)) as bill_amount_usd,
            cast(cost_rate_usd * qty as number(38,2)) as cost_amount_usd,
            cast(cost_rate_cola_usd * qty as number(38,2)) as cost_amount_cola_usd
        from timesheet_entry as te
        left join sage_intacct_employee as intacct_employee on te.employee_id_intacct = intacct_employee.employeeid
        left join sage_intacct_location as intacct_locations on intacct_employee.locationid = intacct_locations.locationid
        left join date_listings_flattened as w on te.dte_entry = w.dte
    ),
    
    -- ************************************************************************************************************************************************
    -- BLANK_DATA: default blank data for employee entity grouping so that all data sets have a value even if 0
    blank_data as (
        select
            te.key_employee, te.key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
            0 as hours,  
            0 as hours_billable,
            
            -- get aggregates for entity
            currency_code_employee_entity as currency_code_entity,
            0  as amt_bill_entity,
            0  as amt_cost_entity,
            0  as amt_cost_cola_entity,
            0 as avg_bill_rate_entity,
            0 as avg_cost_rate_entity,
            0 as avg_cost_rate_cola_entity,
            
            -- get aggregates for USD
            currency_code_employee_usd as currency_code_usd,
            0 as amt_bill_usd,
            0 as amt_cost_usd,
            0 as amt_cost_cola_usd,
            0 as avg_bill_rate_usd,
            0 as avg_cost_rate_usd,
            0 as avg_cost_rate_cola_usd,
        from base_timesheet_entry as te
        group by all
    ),
    
    -- ************************************************************************************************************************************************
    -- BLANK_DATA_PROJECT_ENTITY: default blank data for project entity grouping so that all data sets have a value even if 0
    blank_data_project_entity as (
        select
            te.key_employee, te.key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
            0 as hours,  
            0 as hours_billable,
            
            -- get aggregates for employee entity
            -- get aggregates for entity
            currency_code_employee_entity as currency_code_entity,
            0  as amt_bill_entity,
            0  as amt_cost_entity,
            0  as amt_cost_cola_entity,
            0 as avg_bill_rate_entity,
            0 as avg_cost_rate_entity,
            0 as avg_cost_rate_cola_entity,
            
            -- get aggregates for USD
            currency_code_employee_usd as currency_code_usd,
            0 as amt_bill_usd,
            0 as amt_cost_usd,
            0 as amt_cost_cola_usd,
            0 as avg_bill_rate_usd,
            0 as avg_cost_rate_usd,
            0 as avg_cost_rate_cola_usd,
        from base_timesheet_entry as te
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- TOTAL_DATA: get the sum of all data grouped together with no filters
    total_data as (  
        select
            'Total' as type, 'Employee' as entity_grouping, 1 as type_sort, te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
            sum(te.qty) as hours,
            sum(iff(te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true, te.qty,0)) as hours_billable,
            
            -- get aggregates for entity
            currency_code_employee_entity as currency_code_entity,
            cast(sum(te.bill_amount_employee_entity) as number(38,2))  as amt_bill_entity,
            cast(sum(te.cost_amount_employee_entity) as number(38,2))  as amt_cost_entity,
            cast(sum(te.cost_amount_cola_employee_entity) as number(38,2))  as amt_cost_cola_entity,
            cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
            cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
            cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
            
            -- get aggregates for USD
            'USD' as currency_code_usd,
            cast(sum(te.bill_amount_usd) as number(38,2))  as amt_bill_usd,
            cast(sum(te.cost_amount_usd) as number(38,2))  as amt_cost_usd,
            cast(sum(te.cost_amount_cola_usd) as number(38,2))  as amt_cost_cola_usd,
            cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
            cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
            cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd,
        from base_timesheet_entry as te
        group by all
    ) ,

    -- ************************************************************************************************************************************************
    -- TOTAL_DATA_PROJECT_ENTITY: get the sum of all data grouped by project entity with no filters
    total_data_project_entity as (
        select
            'Total' as type, 'Project' as entity_grouping, 101 as type_sort, te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
            sum(te.qty) as hours,
            sum(iff(te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true, te.qty,0)) as hours_billable,
            
            -- get aggregates for entity
            currency_code_project_entity as currency_code_entity,
            cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
            cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
            cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
            cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
            cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
            cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
            
            -- get aggregates for USD
            'USD' as currency_code_usd,
            cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
            cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
            cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
            cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
            cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
            cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd,
        from base_timesheet_entry as te
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- BILLABLE_DATA: get the sum of all billable data grouped limiting to only billable entries
    billable_data as (
        select 'Billable' as type, 'Employee' as entity_grouping, 2 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, 
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                sum(te.qty) as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as b
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- BILLABLE_DATA_PROJECT_ENTITY: get the sum of all billable data grouped by project entity limiting to only billable entries
    billable_data_project_entity as (
        select 'Billable' as type, 'Project' as entity_grouping, 102 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                sum(te.qty) as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name not in (select phase_code from time_type_phase_codes where time_type = 'billable') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as b
        group by all
    ),  

    -- ************************************************************************************************************************************************
    -- PTO_DATA: get the sum of all pto data grouped by employee entity limiting to only pto entries
    pto_data as (
        select 'PTO' as type, 'Employee' as entity_grouping, 3 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'pto')
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as p
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- PTO_DATA_PROJECT_ENTITY: get the sum of all pto data grouped by project entity limiting to only pto entries
    pto_data_project_entity as (
        select 'PTO' as type, 'Project' as entity_grouping, 103 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'pto')
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as p
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- INTERNAL_DATA: get the sum of all internal data grouped by employee entity limiting to only internal entries
    internal_data as (
        select 'Internal' as type, 'Employee' as entity_grouping, 4 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'internaltvl'))
            or (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable=false)
            or (te.task_name not in (select phase_code from time_type_phase_codes where time_type in ('internal','disfun','nowork')) and te.bln_billable=false)
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as i
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- INTERNAL_DATA_PROJECT_ENTITY: get the sum of all internal data grouped by project entity limiting to only internal entries
    internal_data_project_entity as (
        select 'Internal' as type, 'Project' as entity_grouping, 104 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'internaltvl'))
            or (te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable=false)
            or (te.task_name not in (select phase_code from time_type_phase_codes where time_type in ('internal','disfun','nowork')) and te.bln_billable=false)
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as i
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- TVL_DATA: get the sum of all travel data grouped by employee entity limiting to only travel entries
    tvl_data as (
        select 'TVL' as type, 'Employee' as entity_grouping, 5 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as t
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- TVL_DATA_PROJECT_ENTITY: get the sum of all travel data grouped by project entity limiting to only travel entries
    tvl_data_project_entity as (
        select 'TVL' as type, 'Project' as entity_grouping, 105 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'tvl') and te.bln_billable = true
            group by all
            union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as t
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- DISFUN_DATA: get the sum of all disfun data grouped by employee entity limiting to only disfun entries
    disfun_data as (
         select 'DISFUN' as type, 'Employee' as entity_grouping, 6 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'disfun')
            group by all
        union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as d
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- DISFUN_DATA_PROJECT_ENTITY: get the sum of all disfun data grouped by project entity limiting to only disfun entries
    disfun_data_project_entity as (
         select 'DISFUN' as type, 'Project' as entity_grouping, 106 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'disfun')
            group by all
        union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as d
        group by all
    ),
    
    -- ************************************************************************************************************************************************
    -- NOWORK_DATA: get the sum of all no work data grouped by employee entity limiting to only no work entries
    nowork_data as (
         select 'No Work' as type, 'Employee' as entity_grouping, 6 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_employee as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for employee entity
                currency_code_employee_entity as currency_code_entity,
                cast(sum(te.bill_amount_employee_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_employee_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_employee_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'nowork') and te.bln_billable = false
            group by all
        union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data
            )
        ) as d
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- NOWORK_DATA_PROJECT_ENTITY: get the sum of all nowork data grouped by project entity limiting to only nowork entries
    nowork_data_project_entity as (
         select 'No Work' as type, 'Project' as entity_grouping, 106 as type_sort, key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year,
            sum(hours) as hours,
            sum(hours_billable) as hours_billable,
            
            -- get aggregates for entity
            currency_code_entity,
            sum(amt_bill_entity) as amt_bill_entity,
            sum(amt_cost_entity) as amt_cost_entity,
            sum(amt_cost_cola_entity) as amt_cost_cola_entity,
            sum(avg_bill_rate_entity) as avg_bill_rate_entity,
            sum(avg_cost_rate_entity) as avg_cost_rate_entity,
            sum(avg_cost_rate_cola_entity) as avg_cost_rate_cola_entity,

            -- get aggregates for USD
            'USD' as currency_code_usd,
            sum(amt_bill_usd) as amt_bill_usd,
            sum(amt_cost_usd) as amt_cost_usd,
            sum(amt_cost_cola_usd) as amt_cost_cola_usd,
            sum(avg_bill_rate_usd) as avg_bill_rate_usd,
            sum(avg_cost_rate_usd) as avg_cost_rate_usd,
            sum(avg_cost_rate_cola_usd) as avg_cost_rate_cola_usd
        from (
            select
                te.key_employee, key_entity_project as key_entity, te.employee_id, te.date_group_id, te.date_group_type_id, te.date_group_year,
                sum(te.qty) as hours,
                0 as hours_billable,
                
                -- get aggregates for project entity
                currency_code_project_entity as currency_code_entity,
                cast(sum(te.bill_amount_project_entity) as number(38,2)) as amt_bill_entity,
                cast(sum(te.cost_amount_project_entity) as number(38,2)) as amt_cost_entity,
                cast(sum(te.cost_amount_cola_project_entity) as number(38,2)) as amt_cost_cola_entity,
                cast(div0(amt_bill_entity, hours_billable) as number(38,2)) as avg_bill_rate_entity,
                cast(div0(amt_cost_entity, hours) as number(38,2)) as avg_cost_rate_entity,
                cast(div0(amt_cost_cola_entity, hours) as number(38,2)) as avg_cost_rate_cola_entity,
                
                -- get aggregates for USD
                currency_code_employee_usd as currency_code_usd,
                cast(sum(te.bill_amount_usd) as number(38,2)) as amt_bill_usd,
                cast(sum(te.cost_amount_usd) as number(38,2)) as amt_cost_usd,
                cast(sum(te.cost_amount_cola_usd) as number(38,2)) as amt_cost_cola_usd,
                cast(div0(amt_bill_usd, hours_billable) as number(38,2)) as avg_bill_rate_usd,
                cast(div0(amt_cost_usd, hours) as number(38,2)) as avg_cost_rate_usd,
                cast(div0(amt_cost_cola_usd, hours) as number(38,2)) as avg_cost_rate_cola_usd
            from base_timesheet_entry as te
            where te.task_name in (select phase_code from time_type_phase_codes where time_type = 'nowork')
            group by all
        union (
                select key_employee, key_entity, employee_id, date_group_id, date_group_type_id, date_group_year, hours, hours_billable, currency_code_entity, amt_bill_entity, amt_cost_entity, 
                    amt_cost_cola_entity, avg_bill_rate_entity, avg_cost_rate_entity, avg_cost_rate_cola_entity, currency_code_usd, amt_bill_usd, amt_cost_usd, amt_cost_cola_usd, avg_bill_rate_usd, 
                    avg_cost_rate_usd, avg_cost_rate_cola_usd
                from blank_data_project_entity
            )
        ) as d
        group by all
    ),

    -- ************************************************************************************************************************************************
    -- COMBINED: get the sum of all disfun data grouped by employee entity limiting to only disfun entries
    combined as (
        Select te.*,
            cast(case
                when te.date_group_type_id = 'W' then ifnull(entities.work_hours_per_week,0)
                when te.date_group_type_id = 'M' then ifnull(entities.work_hours_per_week,0) * 52 / 12
                when te.date_group_type_id = 'Q' then ifnull(entities.work_hours_per_week,0) * 52 / 4
                when te.date_group_type_id = 'Y' then ifnull(entities.work_hours_per_week,0) * 52
                else 0
            end as number(38,2)) as work_hours,
            cast(
                case
                    when te.type = 'Total' or te.type = 'Billable' then 
                        case
                            when te.date_group_type_id = 'W' then ifnull(f.plan_hours_per_week,0)
                            when te.date_group_type_id = 'M' then ifnull(f.plan_hours_per_week,0) * 52 / 12
                            when te.date_group_type_id = 'Q' then ifnull(f.plan_hours_per_week,0) * 52 / 4
                            when te.date_group_type_id = 'Y' then ifnull(f.plan_hours_per_year,0)
                            else 0
                        end
                    else 0
                end
                 as number(38,2)) as planned_hours_billable,
            cast(case
                when te.type = 'Total' or te.type = 'Billable' then  ifnull(f.bill_rate_entity, 0)
                else 0
            end as number(38,2))as planned_rate_entity,
            cast(case
                when te.type = 'Total' or te.type = 'Billable' then  ifnull(f.bill_rate_usd, 0)
                else 0
            end as number(38,2)) as planned_rate_usd,
            cast(case
                when te.type = 'Total' or te.type = 'Billable' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(f.plan_bill_amount_per_week_entity, 0)
                        when te.date_group_type_id = 'M' then ifnull(f.plan_bill_amount_per_week_entity, 0) / 12
                        when te.date_group_type_id = 'Q' then ifnull(f.plan_bill_amount_per_week_entity, 0) / 4
                        when te.date_group_type_id = 'Y' then ifnull(f.plan_bill_amount_per_week_entity, 0) * 52
                        else 0
                    end    
                else 0                
            end as number(38,2)) as planned_bill_amount_entity,
            cast(case
                when te.type = 'Total' or te.type = 'Billable' then
                    case
                        when te.date_group_type_id = 'W' then ifnull(f.plan_bill_amount_per_week_usd, 0)
                        when te.date_group_type_id = 'M' then ifnull(f.plan_bill_amount_per_week_usd, 0) / 12
                        when te.date_group_type_id = 'Q' then ifnull(f.plan_bill_amount_per_week_usd, 0) / 4
                        when te.date_group_type_id = 'Y' then ifnull(f.plan_bill_amount_per_week_usd, 0) * 52
                        else 0
                    end    
                else 0            
            end as number(38,2)) as planned_bill_amount_usd
        from (
            select 0 as pto_hours, t.* from total_data t
            union 
                select p.hours pto_hours, b.* 
                from billable_data b 
                left join pto_data p on b.employee_id = p.employee_id 
                    and b.date_group_id=p.date_group_id 
                    and b.date_group_type_id = p.date_group_type_id
            union 
                select p.hours pto_hours, b.* 
                from billable_data_project_entity b 
                left join pto_data_project_entity p on b.employee_id = p.employee_id and b.key_entity = p.key_entity
                    and b.date_group_id=p.date_group_id 
                    and b.date_group_type_id = p.date_group_type_id
            union
                select 0 as pto_hours, i.* from internal_data i
            union
                select 0 as pto_hours, i.* from internal_data_project_entity i
            union 
                select 0 as pto_hours, t.* from tvl_data t
            union 
                select 0 as pto_hours, t.* from tvl_data_project_entity t
            union
                select 0 as pto_hours, p.* from pto_data p
            union
                select 0 as pto_hours, p.* from pto_data_project_entity p
            union
                select 0 as pto_hours, d.* from disfun_data d
            union
                select 0 as pto_hours, d.* from disfun_data_project_entity d
            union
                select 0 as pto_hours, d.* from nowork_data d
            union
                select 0 as pto_hours, d.* from nowork_data_project_entity d
        ) as te
        left join users_forecast f on te.key_employee = f.key_employee and te.date_group_year = f.year
        left join portal_entities entities on te.key_entity = entities.record_id
    
    )
    -- ************************************************************************************************************************************************
    -- Get prod data for timesheet aggregates
    select pto_hours,
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        concat(te.employee_id, te.date_group_id, te.type) as id,
        te.key_employee,
        te.key_entity,
        te.type,
        te.type_sort,
        te.date_group_id,
        te.entity_grouping,
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
        coalesce(e.bln_mst, intacct_employees.mst_member, false) as bln_mst,
        ifnull(e.bln_is_active,false) as bln_active,
        ifnull(upper(e.currency_code),intacct_employees.currency) as currency_employee,
        'USD' as currency_usd,
        currency_code_entity,
        te.hours,
        te.hours_billable,
        te.amt_bill_entity,
        te.amt_cost_entity,
        te.amt_cost_cola_entity,
        te.avg_bill_rate_entity,
        te.avg_cost_rate_cola_entity,
        te.amt_bill_usd,
        te.amt_cost_usd,
        te.amt_cost_cola_usd,
        te.avg_bill_rate_usd,
        te.avg_cost_rate_cola_usd,
        te.work_hours,
        te.planned_hours_billable,
        te.planned_rate_entity,
        te.planned_rate_usd,
        te.planned_bill_amount_entity as amt_bill_planned_entity,
        te.planned_bill_amount_usd as amt_bill_planned_usd
    from combined te
    left join date_groups as dg on te.date_group_id = dg.id
    left join date_groups_types as dgt on te.date_group_type_id = dgt.id
    left join dim_employee as e on te.key_employee = e.key
    left join sage_intacct_employee as intacct_employees on te.employee_id = intacct_employees.employeeid
    left join sage_intacct_location as intacct_locations on intacct_employees.locationid = intacct_locations.locationid
    left join sage_intacct_department as intacct_departments on intacct_employees.departmentkey = intacct_departments.recordno
    left join portal_base_teams as portal_base_teams on intacct_employees.home_region = portal_base_teams.intacct_id
    left join portal_entities as portal_entities on ifnull(intacct_locations.parentkey,intacct_locations.recordno) = portal_entities.id