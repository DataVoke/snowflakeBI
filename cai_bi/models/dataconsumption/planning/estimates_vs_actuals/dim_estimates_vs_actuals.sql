{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="estimates_vs_actuals"
    )
}}

with 
    eva as (
        select 
            key,
            key_assignment,
            key_resource_request,
            key_time_period,
            key_resource,
            key_project_manager,
            key_project,
            key_opportunity,
            owner_id,
            src_created_by,
            src_modified_by,
            time_period_type_id,
            dte_start,
            dte_end,
            dte_system_modstamp,
            dts_src_created,
            dts_src_modified,
            name,
            currency_iso_code_bill_rate,
            actual_hours,
            planned_hours,
            requested_hours,
            planned_bill_rate,
            bln_timecard_is_submitted,
            src_sys_key
        from {{ ref('estimates_vs_actuals') }} 
        where src_sys_key = 'sfc' and bln_exclude_from_planners=false
    ),
    entities as (select id, record_id, currency_id, display_name, ukg_id from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    opportunities as (select key, name, stage_name from {{ ref('sales_opportunity') }} where src_sys_key='sfc'),
    locations as (select record_id, id, intacct_id, display_name, entity_id from {{ source('portal', 'locations') }} where _fivetran_deleted = false and id != '55-1'),
    ukg_employees as (
        select 
            ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id,
            ifnull(sfc.key, por.contact_id) as sfc_contact_id, 

            coalesce(override_entity.record_id, entities.record_id, companies.record_id) as employee_key_entity,
            coalesce(override_entity.display_name, entities.display_name, companies.display_name) as employee_entity_name,
            coalesce(override_entity.currency_id, entities.currency_id, companies.currency_id) as employee_currency,
            practices.record_id as employee_key_practice,
            practices.display_name as employee_practice_name,
            departments.record_id as employee_key_department,
            departments.display_name as employee_department_name,
            employee_types.record_id as employee_key_employee_type,
            employee_types.display_name as employee_employee_type_name,
            ukg.key, ukg.link, ukg.key_entity, ukg.hash_link, ukg.src_sys_key, ukg.status, initcap(ukg.display_name) as display_name, initcap(ukg.display_name_lf) as display_name_lf, ukg.email_address_work, int.intacct_employee_id, ukg.dts_last_hire, ukg.dte_src_end, ukg.dte_src_start
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }} as sfc on ukg.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }} as por on ukg.hash_link = por.hash_link and por.src_sys_key = 'por'
        left join {{ ref('employee') }} as int on ukg.hash_link = int.hash_link and int.src_sys_key = 'int'
        left join locations as por_loc on int.location_id_intacct = por_loc.intacct_id
        left join entities on por_loc.entity_id = entities.id
        left join entities companies on ukg.key_entity = companies.ukg_id
        left join entities override_entity on por.intacct_override_entity_id = override_entity.display_name
        left join {{ source('portal', 'practices') }} on ukg.practice_id = practices.ukg_id
        left join {{ source('portal', 'departments') }} on ukg.department_id = departments.ukg_id
        left join {{ source('portal', 'employee_types') }} on ukg.employee_type_id = employee_types.ukg_id
        where ukg.src_sys_key = 'ukg'
    ),
    projects as (
        select p.key, p.link, i_p.key_entity, i_p.project_name, i_p.project_id, i_p.project_status, i_p.currency_iso_code,i_p.location_id_intacct, 
            por_dep.record_id as project_key_department,
            por_dep.display_name as project_department_name,
            ifnull(por_pract.record_id,por_pract_bkup.record_id) as project_key_practice,
            ifnull(por_pract.display_name, por_pract_bkup.display_name) as project_practice_name,
            por_pract_area.record_id as project_key_practice_area,
            por_pract_area.display_name as project_practice_area_name,
            account.key as project_key_client_site,
            account.name as project_client_site_name,
            coalesce(parent_account8.key,parent_account7.key,parent_account6.key,parent_account5.key,parent_account4.key,parent_account3.key,parent_account2.key,parent_account1.key) as project_key_client,
            coalesce(parent_account8.name, parent_account7.name, parent_account6.name, parent_account5.name, parent_account4.name, parent_account3.name, parent_account2.name, parent_account1.name) as project_client_name,
            emp.link as project_key_client_manager,
            emp.display_name as project_client_manager_name,
            emp.display_name as project_client_manager_name_lf,
            emp.email_address_work as project_client_manager_email,
            project_entity.record_id as project_key_entity,
            project_entity.display_name as project_entity_name,
            project_entity.currency_id as project_entity_currency_id,
            project_location.record_id as project_key_location,
            project_location.display_name as project_location_name
        from {{ ref('project') }} p
        left join {{ ref('project') }} i_p on p.hash_link = i_p.hash_link and i_p.src_sys_key = 'int'
        left join {{ source('portal', 'departments') }} as por_dep on i_p.department_id = por_dep.intacct_id
        left join {{ source('portal', 'practice_areas') }} as por_pract_area on i_p.department_id = por_pract_area.intacct_id
        left join {{ source('portal', 'practices') }} as por_pract on por_pract_area.practice_id = por_pract.id
        left join {{ source('portal', 'practices') }} as por_pract_bkup on p.practice_id = por_pract_bkup.salesforce_id
        left join entities project_entity on i_p.key_entity = project_entity.id
        left join {{ ref('sales_account') }} as account on p.account_id = account.key
        left join {{ ref('sales_account') }} parent_account1 on account.key_parent_account = parent_account1.key
        left join {{ ref('sales_account') }} parent_account2 on parent_account1.key_parent_account = parent_account2.key
        left join {{ ref('sales_account') }} parent_account3 on parent_account2.key_parent_account = parent_account3.key
        left join {{ ref('sales_account') }} parent_account4 on parent_account3.key_parent_account = parent_account4.key
        left join {{ ref('sales_account') }} parent_account5 on parent_account4.key_parent_account = parent_account5.key
        left join {{ ref('sales_account') }} parent_account6 on parent_account5.key_parent_account = parent_account6.key
        left join {{ ref('sales_account') }} parent_account7 on parent_account6.key_parent_account = parent_account7.key
        left join {{ ref('sales_account') }} parent_account8 on parent_account7.key_parent_account = parent_account8.key
        left join {{ ref('sales_account') }} parent_account9 on parent_account8.key_parent_account = parent_account9.key
        left join ukg_employees emp on p.client_manager_id = emp.sfc_contact_id
        left join locations project_location on i_p.location_id_intacct = project_location.intacct_id
        where p.src_sys_key='sfc'
    ),
    vw_employee_pay as (select key as pay_key,key_employee, date_from, date_to, hourly_pay_rate_cola_original, hourly_pay_rate_cola_usd, hourly_pay_rate_cola_entity, currency_code_original 
                        from {{ ref('vw_employee_pay_history') }}
    ),
    currency_conversion as (
                    select 
                        frm_curr, 
                        to_curr, 
                        date, 
                        fx_rate_mul
                    from {{ ref("ref_fx_rates_timeseries") }}  as cc
                    where frm_curr in (select currency from {{ ref('currencies_active') }})
                    and to_curr in (select currency from {{ ref('currencies_active') }})
    ),
    timeentry as (select int.key,
                        int.key_project,
                        int.project_id,
                        employee_int.link as key_employee,
                        int.dte_entry,
                        int.task_name,
                        
                        --get hours
                        iff(int.task_name != 'NOWORK', int.qty, 0) as hours_total, 
                        iff(int.bln_billable = true and int.task_name = 'TVL', int.qty, 0) as hours_tvl,
                        iff(int.bln_billable = true and int.task_name != 'TVL', int.qty, 0) as hours_billable_non_travel,
                        
                        -- get currency codes
                        upper(coalesce(nullif(int.currency_code,''),nullif(p.currency_iso_code,''),'USD')) as int_currency_code_original,

                        --get bill rates
                        cast(ifnull(int.bill_rate,0) as number(38,2)) as bill_rate_original, 
                    from {{ ref('timesheet_entry') }} int
                    left join ukg_employees as employee_int on int.employee_id_intacct = employee_int.intacct_employee_id
                    left join {{ ref('project') }} p on int.key_project = p.key
                    where int.src_sys_key = 'int'
    ), 
    base as (
        select 
            eva.key,
            eva.key_assignment,
            emp.employee_key_entity as key_entity_employee,
            p.project_key_entity as key_entity_project,
            pm.key as key_project_manager,
            p.link as key_project,
            emp.key as key_employee,
            p.project_key_location as key_location_project,
            p.project_key_practice as key_practice_project,
            emp.employee_key_practice as key_practice_employee,
            p.project_key_practice_area as key_practice_area_project,
            p.project_key_department as key_department_project,
            emp.employee_key_department as key_department_employee,
            p.project_key_client as key_top_level_account,
            p.project_key_client_site as key_account,
            emp.employee_key_employee_type as key_employee_type,
            p.project_key_client_manager as key_client_site_manager,
            eva.key_resource_request,
            eva.key_time_period,
            eva.owner_id,
            eva.src_created_by,
            eva.src_modified_by,
            eva.time_period_type_id,
            eva.key_project_manager as sfc_project_manager_id,
            eva.key_project as sfc_project_id,
            eva.key_resource as sfc_resource_id,
            eva.dte_start,
            eva.dte_end,
            eva.dte_system_modstamp,
            eva.dts_src_created,
            eva.dts_src_modified,
            eva.name,
            eva.bln_timecard_is_submitted,
            emp.display_name as employee_name,
            emp.display_name_lf as employee_name_lf,
            emp.email_address_work as employee_name_email,
            emp.dte_src_start as dte_employee_start,
            date(emp.dts_last_hire) as dte_employee_last_hire,
            emp.dte_src_end as dte_employee_end,
            emp.status as employee_status,
            pm.display_name as project_manager_name,
            pm.display_name_lf as project_manager_name_lf,
            pm.email_address_work as project_manager_email,
            p.project_name,
            p.project_id,
            p.project_status,
            p.project_entity_name as entity_name_project,
            emp.employee_entity_name as entity_name_employee,
            p.project_location_name as location_name_project,
            emp.employee_practice_name as practice_name_employee,
            p.project_practice_name as practice_name_project,
            p.project_practice_area_name as practice_area_name_project,
            p.project_client_name as top_level_account_name,
            p.project_client_site_name as account_name,
            p.project_department_name as department_name_project,
            emp.employee_department_name as department_name_employee,
            emp.employee_employee_type_name as employee_type_name,
            p.project_client_manager_name as client_manager_name,
            p.project_client_manager_name_lf as client_manager_name_lf,
            p.project_client_manager_email as client_manager_email,
            o.name as opportunity_name,
            o.stage_name as opportunity_status,
            iff(eva.dte_start >= current_date(), current_date()-1, eva.dte_start) as dte_conversion_rate,

            --************************************************************************************************************************************************
            --CURRENCY INFORMATION
            --************************************************************************************************************************************************
            upper(eva.currency_iso_code_bill_rate) as currency_iso_code_project,
            upper(emp.employee_currency) as currency_iso_code_entity_employee,
            upper(p.project_entity_currency_id) as currency_iso_code_entity_project,
            upper(pay.currency_code_original) as currency_iso_code_pay,

            --************************************************************************************************************************************************
            --CONVERSION RATES
            --************************************************************************************************************************************************
            cast(ifnull(sfc_cc_to_usd.fx_rate_mul,1) as number(38,6)) as conversion_rate_sfc_usd,
            cast(ifnull(sfc_cc_to_entity_employee.fx_rate_mul,1) as number(38,6)) as conversion_rate_sfc_entity_employee,
            cast(ifnull(sfc_cc_to_entity_project.fx_rate_mul,1) as number(38,6)) as conversion_rate_sfc_entity_project,
            cast(ifnull(pay_cc_to_sfc.fx_rate_mul,1) as number(38,6)) as conversion_rate_pay_to_sfc,
            cast(ifnull(pay_cc_to_usd.fx_rate_mul,1) as number(38,6)) as conversion_rate_pay_to_usd,
            cast(ifnull(pay_cc_to_entity_employee.fx_rate_mul,1) as number(38,6)) as conversion_rate_pay_to_entity_employee,
            cast(ifnull(pay_cc_to_entity_project.fx_rate_mul,1) as number(38,6)) as conversion_rate_pay_to_entity_project,
            cast(ifnull(cc_int_bill_to_project.fx_rate_mul,1) as number(38,6)) as conversion_rate_time_to_project,

            --************************************************************************************************************************************************
            --HOURS
            --************************************************************************************************************************************************
            --Salesforce Hours
            cast(ifnull(eva.actual_hours, 0) as number(38,2)) as hours_actual_sfc,
            cast(ifnull(
                case when dte_employee_end is null then eva.planned_hours
                    when dte_employee_end < eva.dte_start then 0 --  0 out hours if the employee end date is before the start date of it eva period.
                    else eva.planned_hours
                end
            , 0) as number(38,2)) as hours_planned_sfc,
            cast(ifnull(
                case when dte_employee_end is null then eva.requested_hours
                    when dte_employee_end <= eva.dte_start then 0 --  0 out hours if the employee end date is before the start date of it eva period
                    else eva.requested_hours
                end
            , 0) as number(38,2)) as hours_requested_sfc,
            
            -- Intacct Actual Hours
            cast(sum(ifnull(timeentry.hours_billable_non_travel, 0)) as number(38,2)) as hours_actual_billable_int,
            cast(sum(ifnull(timeentry.hours_tvl, 0)) as number(38,2)) as hours_actual_tvl_int,
            cast(sum(ifnull(timeentry.hours_total, 0)) as number(38,2)) as hours_actual_total_int,

            --************************************************************************************************************************************************
            --BILL RATES SALESFORCE
            --************************************************************************************************************************************************
            cast(ifnull(eva.planned_bill_rate,0) as number(38,2)) as rate_bill_sfc,
            cast(ifnull(rate_bill_sfc * conversion_rate_sfc_usd, 0) as number(38,2)) as rate_bill_usd_sfc,            
            cast(ifnull(rate_bill_sfc * conversion_rate_sfc_entity_project, 0) as number(38,2)) as rate_bill_entity_project_sfc,
            cast(ifnull(rate_bill_sfc * conversion_rate_sfc_entity_employee, 0) as number(38,2)) as rate_bill_entity_employee_sfc,
            
            --************************************************************************************************************************************************
            --BILL AMOUNT SALESFORCE (PLANNED)
            --************************************************************************************************************************************************
            cast(ifnull(rate_bill_sfc * hours_planned_sfc, 0) as number(38,2)) as amt_planned_sfc,
            cast(ifnull(rate_bill_usd_sfc * hours_planned_sfc, 0) as number(38,2)) as amt_planned_usd_sfc,            
            cast(ifnull(rate_bill_entity_project_sfc * hours_planned_sfc, 0) as number(38,2)) as amt_planned_entity_project_sfc,
            cast(ifnull(rate_bill_entity_employee_sfc * hours_planned_sfc, 0) as number(38,2)) as amt_planned_entity_employee_sfc,
            
            --************************************************************************************************************************************************
            --BILL AMOUNT SALESFORCE (ACTUAL)
            --************************************************************************************************************************************************
            cast(ifnull(rate_bill_sfc * hours_actual_sfc, 0) as number(38,2)) as amt_actual_sfc,
            cast(ifnull(rate_bill_usd_sfc * hours_actual_sfc, 0) as number(38,2)) as amt_actual_usd_sfc,            
            cast(ifnull(rate_bill_entity_project_sfc * hours_actual_sfc, 0) as number(38,2)) as amt_actual_entity_project_sfc,
            cast(ifnull(rate_bill_entity_employee_sfc * hours_actual_sfc, 0) as number(38,2)) as amt_actual_entity_employee_sfc,

            --************************************************************************************************************************************************
            --BILL RATES INTACCT (BILLABLE)
            --************************************************************************************************************************************************
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1)) * timeentry.hours_billable_non_travel as number(38,2))),sum(timeentry.hours_billable_non_travel)), 0) as number(38,2)) as rate_bill_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_usd,1)) * timeentry.hours_billable_non_travel as number(38,2))),sum(timeentry.hours_billable_non_travel)), 0) as number(38,2)) as rate_bill_usd_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_project,1)) * timeentry.hours_billable_non_travel as number(38,2))),sum(timeentry.hours_billable_non_travel)), 0) as number(38,2)) as rate_bill_entity_project_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_employee,1)) * timeentry.hours_billable_non_travel as number(38,2))),sum(timeentry.hours_billable_non_travel)), 0) as number(38,2)) as rate_bill_entity_employee_int,

            --************************************************************************************************************************************************
            --BILL AMOUNT INTACCT (BILLABLE)
            --************************************************************************************************************************************************
            cast(ifnull(rate_bill_int * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_int,
            cast(ifnull(rate_bill_usd_int * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_usd_int,
            cast(ifnull(rate_bill_entity_project_int * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_entity_project_int,
            cast(ifnull(rate_bill_entity_employee_int * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_entity_employee_int,

            --************************************************************************************************************************************************
            --BILL RATES INTACCT (TVL)
            --************************************************************************************************************************************************
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1)) * timeentry.hours_tvl as number(38,2))),sum(timeentry.hours_tvl)), 0) as number(38,2)) as rate_bill_tvl_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_usd,1)) * timeentry.hours_tvl as number(38,2))),sum(timeentry.hours_tvl)), 0) as number(38,2)) as rate_bill_tvl_usd_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_project,1)) * timeentry.hours_tvl as number(38,2))),sum(timeentry.hours_tvl)), 0) as number(38,2)) as rate_bill_tvl_entity_project_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_employee,1)) * timeentry.hours_tvl as number(38,2))),sum(timeentry.hours_tvl)), 0) as number(38,2)) as rate_bill_tvl_entity_employee_int,

            --************************************************************************************************************************************************
            --BILL AMOUNT INTACCT (TVL)
            --************************************************************************************************************************************************
            cast(ifnull(rate_bill_tvl_int * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_int,
            cast(ifnull(rate_bill_tvl_usd_int * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_usd_int,
            cast(ifnull(rate_bill_tvl_entity_project_int * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_entity_project_int,
            cast(ifnull(rate_bill_tvl_entity_employee_int * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_entity_employee_int,

            --************************************************************************************************************************************************
            --BILL RATES INTACCT (TOTAL)
            --************************************************************************************************************************************************
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1)) * timeentry.hours_total as number(38,2))),sum(timeentry.hours_total)), 0) as number(38,2)) as rate_bill_total_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_usd,1)) * timeentry.hours_total as number(38,2))),sum(timeentry.hours_total)), 0) as number(38,2)) as rate_bill_total_usd_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_project,1)) * timeentry.hours_total as number(38,2))),sum(timeentry.hours_total)), 0) as number(38,2)) as rate_bill_total_entity_project_int,
            cast(ifnull(div0(sum(cast((timeentry.bill_rate_original * ifnull(conversion_rate_time_to_project,1) * ifnull(conversion_rate_sfc_entity_employee,1)) * timeentry.hours_total as number(38,2))),sum(timeentry.hours_total)), 0) as number(38,2)) as rate_bill_total_entity_employee_int,

            --************************************************************************************************************************************************
            --BILL AMOUNT INTACCT (TVL)
            --************************************************************************************************************************************************
            cast(ifnull(rate_bill_total_int * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_int,
            cast(ifnull(rate_bill_total_usd_int * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_usd_int,
            cast(ifnull(rate_bill_total_entity_project_int * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_entity_project_int,
            cast(ifnull(rate_bill_total_entity_employee_int * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_entity_employee_int,
            
            --************************************************************************************************************************************************
            --PAY RATES
            --************************************************************************************************************************************************
            --Rates from UKG pay infomration
            cast(ifnull(cast(ifnull(pay.hourly_pay_rate_cola_original,0) as number(38,2)) * conversion_rate_pay_to_sfc, 0) as number(38,2)) as rate_pay_ukg,
            cast(ifnull(rate_pay_ukg * conversion_rate_pay_to_usd, 0) as number(38,2)) as rate_pay_usd_ukg,
            cast(ifnull(rate_pay_ukg * conversion_rate_pay_to_entity_project, 0) as number(38,2)) as rate_pay_entity_project_ukg,
            cast(ifnull(rate_pay_ukg * conversion_rate_pay_to_entity_employee, 0) as number(38,2)) as rate_pay_entity_employee_ukg,

            --************************************************************************************************************************************************
            --PAY AMOUNT UKG (BILLABLE)
            --************************************************************************************************************************************************
            cast(ifnull(rate_pay_ukg * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_ukg,
            cast(ifnull(rate_pay_usd_ukg * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_usd_ukg,
            cast(ifnull(rate_pay_entity_project_ukg * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_entity_project_ukg,
            cast(ifnull(rate_pay_entity_employee_ukg * hours_actual_billable_int, 0) as number(38,2)) as amt_actual_entity_employee_ukg,
            
            --************************************************************************************************************************************************
            --PAY AMOUNT UKG (TVL)
            --************************************************************************************************************************************************
            cast(ifnull(rate_pay_ukg * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_ukg,
            cast(ifnull(rate_pay_usd_ukg * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_usd_ukg,
            cast(ifnull(rate_pay_entity_project_ukg * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_entity_project_ukg,
            cast(ifnull(rate_pay_entity_employee_ukg * hours_actual_tvl_int, 0) as number(38,2)) as amt_actual_tvl_entity_employee_ukg,

             --************************************************************************************************************************************************
            --PAY AMOUNT UKG (TOTAL)
            --************************************************************************************************************************************************
            cast(ifnull(rate_pay_ukg * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_ukg,
            cast(ifnull(rate_pay_usd_ukg * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_usd_ukg,
            cast(ifnull(rate_pay_entity_project_ukg * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_entity_project_ukg,
            cast(ifnull(rate_pay_entity_employee_ukg * hours_actual_total_int, 0) as number(38,2)) as amt_actual_total_entity_employee_ukg,
            
        from eva
        left join ukg_employees as emp on eva.key_resource = emp.sfc_contact_id
        left join ukg_employees as pm on eva.key_project_manager = pm.sfc_contact_id
        left join projects p on eva.key_project = p.key
        left join opportunities o on eva.key_opportunity = o.key
        left join vw_employee_pay pay on emp.key = pay.key_employee and eva.dte_start between pay.date_from and pay.date_to
        left join timeentry on emp.key = timeentry.key_employee and p.link = timeentry.key_project and timeentry.dte_entry between eva.dte_start and eva.dte_end
        left join currency_conversion as sfc_cc_to_usd on (
                                                            sfc_cc_to_usd.frm_curr =  currency_iso_code_project
                                                            and sfc_cc_to_usd.to_curr = 'USD'
                                                            and sfc_cc_to_usd.date = dte_conversion_rate
                                                        )  
        left join currency_conversion as sfc_cc_to_entity_employee on (
                                                            sfc_cc_to_entity_employee.frm_curr =  currency_iso_code_project
                                                            and sfc_cc_to_entity_employee.to_curr = currency_iso_code_entity_employee
                                                            and sfc_cc_to_entity_employee.date = dte_conversion_rate 
                                                        )  
        left join currency_conversion as sfc_cc_to_entity_project on (
                                                            sfc_cc_to_entity_project.frm_curr =  currency_iso_code_project
                                                            and sfc_cc_to_entity_project.to_curr = currency_iso_code_entity_project
                                                            and sfc_cc_to_entity_project.date = dte_conversion_rate  
                                                        )  
        left join currency_conversion as pay_cc_to_sfc on (
                                                            pay_cc_to_sfc.frm_curr = currency_iso_code_pay
                                                            and pay_cc_to_sfc.to_curr = currency_iso_code_project
                                                            and pay_cc_to_sfc.date = dte_conversion_rate  
                                                        )
        left join currency_conversion as pay_cc_to_usd on (
                                                            pay_cc_to_usd.frm_curr = currency_iso_code_project
                                                            and pay_cc_to_usd.to_curr = 'USD'
                                                            and pay_cc_to_usd.date = dte_conversion_rate  
                                                        )
        left join currency_conversion as pay_cc_to_entity_project on (
                                                            pay_cc_to_entity_project.frm_curr = currency_iso_code_project
                                                            and pay_cc_to_entity_project.to_curr = currency_iso_code_entity_project
                                                            and pay_cc_to_entity_project.date = dte_conversion_rate  
                                                        )
        left join currency_conversion as pay_cc_to_entity_employee on (
                                                            pay_cc_to_entity_employee.frm_curr = currency_iso_code_project
                                                            and pay_cc_to_entity_employee.to_curr = currency_iso_code_entity_employee
                                                            and pay_cc_to_entity_employee.date = dte_conversion_rate  
                                                        )
        left join currency_conversion as cc_int_bill_to_project on (
                                                            cc_int_bill_to_project.frm_curr = int_currency_code_original
                                                            and cc_int_bill_to_project.to_curr = currency_iso_code_project
                                                            and cc_int_bill_to_project.date = dte_conversion_rate
                                                        )
        
        group by all
    )    

Select * from base