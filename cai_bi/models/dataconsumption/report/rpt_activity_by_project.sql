{{
    config(
        alias="activity_by_project",
        materialized="table",
        schema="dataconsumption"
    )
}}

with
    --*********************************************************************************************************
    -- Currency Conversion reference query
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_mul
        from {{ ref('ref_fx_rates_timeseries')}}  as cc
        where frm_curr in (select currency from {{ ref("currencies_active") }})
        and to_curr in (select currency from {{ ref("currencies_active") }})
    ),

    --*********************************************************************************************************
    -- Account reference query
    account as ( 
        select 
            key, 
            name as account_name, 
            key_top_level_parent_account, 
            top_level_parent_account_name 
        from {{ ref("dim_sales_account") }}
    ),

    --*********************************************************************************************************
    -- Project reference query
    project as ( 
        select 
            key, 
            location_id_intacct,
            project_id,
            location_name, 
            group_name, 
            entity_name, 
            practice_name, 
            project_manager_name, 
            project_manager_name_lf, 
            email_address_work as project_manager_email, 
            email_address_personal as project_manager_personal_email, 
            client_site_id, 
            client_manager_id, 
            client_manager_name, 
            client_manager_name_lf, 
            client_manager_email,
            assistant_project_manager_id, 
            assistant_project_manager_name, 
            assistant_project_manager_name_lf, 
            assistant_project_manager_email, 
            project_name, 
            project_status,
            practice_area_name,
            department_name, 
            customer_id, 
            customer_name, 
            practice_id_intacct, 
            billing_type, 
            root_parent_name, 
            amt_po, 
            amt_po_usd, 
            currency_iso_code, 
            key_location, 
            key_practice, 
            key_practice_area, 
            account_id
        from {{ ref("dim_project") }} p
        where dte_src_start is not null
    ),
    --*********************************************************************************************************
    -- Empoyee reference query
    employee as (
        select 
            key, 
            ukg_employee_number,
            email_address_work 
        from {{ ref("dim_employee") }}
    ),

    --*********************************************************************************************************
     -- time entry reference query
    timesheet_entry as (
        select 
            te.key,
            te.key_project,
            te.key_employee,
            cast(ifnull(cc_usd.fx_rate_mul,1) as number(38,6)) as te_currency_conversion_project_usd,
            cast(ifnull(cc_proj.fx_rate_mul,1) as number(38,6)) as te_currency_conversion_project, 
            te.task_key as key_task, 
            te.key_timesheet as key_parent, 
            te.employee_name,
            te.employee_name_lf,
            te.employee_name_lf as employee_name_lf_filter, 
            te.employee_name as employee_name_filter, 
            te.key as record_id, 
            p.currency_iso_code as currency_iso_code_project, 
            null as currency_iso_code_base,
            te.currency_iso_code,
            te.dte_entry, 
            te.qty, 
            te.task_name, 
            te.notes,  
            coalesce(round(te.bill_rate,2),0) as rate, 
            coalesce(round(te.bill_rate * te_currency_conversion_project,2),0) as rate_project, 
            round(rate_project * te_currency_conversion_project_usd,2) as rate_project_usd, 
            round(coalesce( rate * te.qty,0),2) as cost, 
            round(coalesce(rate_project * te.qty,0),2) as cost_project, 
            round(rate_project_usd * te.qty, 2) as cost_project_usd, 
            te.state as status, 
            te.bln_billable as bln_billable, 
            te.sfc_status
        from {{ ref("dim_timesheet_entry") }} as te
        inner join project p on te.key_project = p.key
        left join currency_conversion cc_proj on (
                                                    cc_proj.frm_curr = te.currency_iso_code 
                                                    and cc_proj.to_curr = upper(ifnull(p.currency_iso_code,'USD'))
                                                    and cc_proj.date = te.dte_entry
                                                )
        left join currency_conversion cc_usd on (
                                                    cc_usd.frm_curr = upper(ifnull(p.currency_iso_code,'USD'))
                                                    and cc_usd.to_curr = 'USD'
                                                    and cc_usd.date = te.dte_entry
                                                )
    ),

    --*********************************************************************************************************
     -- Expense item reference query
    expense_item as (
        select 
            p.key as key_project, 
            ei.key as key_expense_item, 
            ei.key_expense as key_expense, 
            ei.key_employee,
            cast(ifnull(cc_usd.fx_rate_mul,1) as number(38,6)) as ex_currency_conversion_project_usd,
            cast(ifnull(cc_proj.fx_rate_mul,1) as number(38,6)) as ex_currency_conversion_project, 
            case 
                when ei.employee_name_lf is null or ei.employee_name_lf ='' then exp_record_id 
                else ei.employee_name_lf ||' - ' || exp_record_id 
            end as employee_name_lf,        
            case 
                when ei.employee_name is null or ei.employee_name ='' then exp_record_id 
                else ei.employee_name ||' - ' || exp_record_id 
            end as employee_name,
            ei.employee_name_lf as employee_name_lf_filter,
            ei.employee_name as employee_name_filter,
            exp_record_id as record_id, 
            upper(ifnull(nullif(p.currency_iso_code,''),'USD')) as currency_iso_code_project, --project current
            upper(ifnull(nullif(ei.exp_base_currency,''),'USD')) as  currency_iso_code_base, -- Reimbursment currency EXPENSE - BASECURR
            upper(ifnull(nullif(ei.org_currency,''),ei.currency_iso_code)) as currency_iso_code,  -- transaction currency -EXPENSE - ORG_CURRENCY
            ei.amt,
            ei.amt_org,
            case
               when upper(nullif(ei.org_currency,'')) is null and upper(ifnull(nullif(ei.org_currency,''),ei.currency_iso_code)) != upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then -1
               when upper(nullif(ei.org_currency,'')) is null then 0
               when upper(ifnull(nullif(ei.org_currency,''),ei.currency_iso_code)) = upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then 1 -- use amt_org
               when upper(ifnull(nullif(ei.currency_iso_code,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))  then 2 -- use amt
               when upper(ifnull(nullif(ei.exp_currency,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))  then 3 -- amt_trx
               else 4
            end as curr_ind, 
            round(ifnull(
                case 
                    when curr_ind = 0 then ifnull(amt_org, amt_trx) 
                    when curr_ind = 1 then amt_org
                    when curr_ind = 2 then amt
                    when curr_ind = 3 then amt_trx
                    else  ifnull(amt_org, amt_trx) -- should handle curr_ind = -1
                end,0)
            ,2) as rate,    
            round(ifnull(
                case 
                    when curr_ind = 0 then ifnull(amt_org, amt_trx) 
                    when curr_ind = 1 then  amt_org
                    when curr_ind = 2 then amt
                    when curr_ind = 3 then amt_trx
                    else  ifnull(amt_org, amt_trx) * ex_currency_conversion_project -- should handle curr_ind = -1
                end,0)
            ,2) as rate_project,
            round(ifnull(rate_project * ex_currency_conversion_project_usd,0),2) as rate_project_usd,
            round(ifnull(
                case 
                    when curr_ind = 0 then ifnull(amt_org, amt_trx) 
                    when curr_ind = 1 then  amt_org
                    when curr_ind = 2 then amt
                    when curr_ind = 3 then amt_trx
                    else  ifnull(amt_org, amt_trx) -- should handle curr_ind = -1
                end,0)
            ,2) as cost,
            round(ifnull(
                case
                    when curr_ind = 0 then ifnull(amt_org, amt_trx) 
                    when curr_ind = 1 then  amt_org
                    when curr_ind = 2 then amt
                    when curr_ind = 3 then amt_trx
                    else ifnull(amt_org, amt_trx) * ex_currency_conversion_project -- should handle curr_ind = -1
                end,0)
            ,2) as cost_project,        
            round(ifnull(cost_project * ex_currency_conversion_project_usd,0),2) as cost_project_usd,
            coalesce( ei.dte_org_exchrate,ei.dte_entry,ei.exp_dte_when_posted) as dte_exch_rate,
            ei.exp_dte_when_posted as dte_entry,
            ei.state as status,
            ei.bln_billable as bln_billable
        from {{ ref("dim_expense_item") }} as ei
        inner join project p on ei.key_project = p.key
        left join currency_conversion cc_proj on (
                                                cc_proj.frm_curr = upper(ifnull(nullif(ei.org_currency,''),ei.exp_base_currency)) 
                                                and cc_proj.to_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                and cc_proj.date = coalesce(ei.dte_org_exchrate,ei.dte_entry,ei.exp_dte_when_posted)
                                            )
        left join currency_conversion cc_usd on (
                                                cc_usd.frm_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                and cc_usd.to_curr = 'USD'
                                                and cc_usd.date = coalesce(ei.dte_org_exchrate,ei.dte_entry,ei.exp_dte_when_posted)
                                            )
        where ei.bln_billable =true and ei.bln_line_item=true
    ), 

     --*********************************************************************************************************
     -- AP Bill item reference query
    ap_bill_item as ( 
        select 
            p.key as key_project,
            apbi.key as key_api, 
            apbi.key_ap_bill,  
            apbi.key_employee,
            cast(ifnull(cc_usd.fx_rate_mul,1) as number(38,6)) as ap_currency_conversion_project_usd,
            cast(ifnull(cc_proj.fx_rate_mul,1) as number(38,6)) as ap_currency_conversion_project, 
            ap_record_id as record_id,
            upper(ifnull(nullif(p.currency_iso_code,''),'USD')) as currency_iso_code_project, 
            upper(ifnull(nullif(apbi.base_currency,''),'USD')) as  currency_iso_code_base,
            upper(ifnull(nullif(apbi.currency_code,''),'USD')) as currency_iso_code,
            case
               when upper(ifnull(nullif(apbi.base_currency,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then 1
               when upper(ifnull(nullif(apbi.currency_code,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then 2
               else 3
            end as curr_ind, 
            coalesce(round(apbi.amt,2),0) as amt, 
            coalesce(round(apbi.amt_trx,2),0) as amt_trx, 
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx
                    end
            ,2) as rate,            
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx * ap_currency_conversion_project
                end
            ,2) as rate_project,
            round(rate_project * ap_currency_conversion_project_usd,2) as rate_project_usd,
            round(case 
                        when curr_ind =1 then amt 
                        when curr_ind =2 then amt_trx
                        else amt_trx
                end
            ,2) as cost,
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx * ap_currency_conversion_project
                end
            ,2) as cost_project,        
            round(cost_project * ap_currency_conversion_project_usd,2) as cost_project_usd,
            coalesce( apbi.dte_exch_rate,apbi.dte_entry,apbi.ap_dte_when_posted) as dte_exch_rate, 
            apbi.ap_dte_when_posted as dte_entry,
            apbi.state as status,
            apbi.bln_billable as bln_billable,
        from {{ ref("dim_ap_bill_item") }} apbi
        inner join project p on apbi.key_project = p.key
        left join currency_conversion cc_proj on (
                                                    cc_proj.frm_curr = upper(ifnull(nullif(apbi.currency_code,''),'USD'))
                                                    and cc_proj.to_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                    and cc_proj.date = coalesce( apbi.dte_exch_rate,apbi.dte_entry,apbi.ap_dte_when_posted)
                                                )
        left join currency_conversion cc_usd on (
                                                    cc_usd.frm_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                    and cc_usd.to_curr = 'USD'
                                                    and cc_usd.date = coalesce( apbi.dte_exch_rate,apbi.dte_entry,apbi.ap_dte_when_posted)
                                                )
        where apbi.bln_billable = true  
    ),

    --*********************************************************************************************************
     -- Credit Card Transaction item reference query
    ccte_entry as (
        select 
            p.key as key_project, 
            ccte.key as key_ccte, 
            ccte.key_cc_transaction,
            ccte.key_employee,
            cast(ifnull(cc_usd.fx_rate_mul,1) as number(38,6)) as ccte_currency_conversion_project_usd,
            cast(ifnull(cc_proj.fx_rate_mul,1) as number(38,6)) as ccte_currency_conversion_project, 
            case 
                when ccte.employee_name_lf is null or ccte.employee_name_lf ='' then ccte.key 
                else ccte.employee_name_lf ||' - ' || ccte.key 
            end as employee_name_lf,
            case 
                when ccte.employee_name is null or ccte.employee_name ='' then ccte.key 
                else ccte.employee_name ||' - ' || ccte.key 
            end as employee_name,
            ccte.employee_name_lf as employee_name_lf_filter,
            ccte.employee_name as employee_name_filter,
            ccte.key as record_id,
            coalesce(round(ccte.amt,2),0) as amt, 
            coalesce(round(ccte.amt_trx,2),0) as amt_trx,
            upper(ifnull(nullif(p.currency_iso_code,''),'USD')) as currency_iso_code_project, 
            upper(ifnull(nullif(ccte.base_currency,''),'USD')) as  currency_iso_code_base, 
            upper(ifnull(nullif(ccte.currency,''),'USD')) as currency_iso_code,
            case
               when upper(ifnull(nullif(ccte.base_currency,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then 1
               when upper(ifnull(nullif(ccte.currency,''),'USD')) = upper(ifnull(nullif(p.currency_iso_code,''),'USD')) then 2
               else 3
            end as curr_ind, 
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx
                end
            , 2) as rate,            
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx * ccte_currency_conversion_project
                end
            , 2) as rate_project,
            round(rate_project * ccte_currency_conversion_project_usd,2) as rate_project_usd,
            round(
                case when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx
                end
            ,2) as cost,
            round(
                case 
                    when curr_ind =1 then amt 
                    when curr_ind =2 then amt_trx
                    else amt_trx * ccte_currency_conversion_project
                end
            , 2) as cost_project,        
            round(cost_project * ccte_currency_conversion_project_usd, 2) as cost_project_usd,          
            ccte.dts_src_created as dte_exch_rate, 
            ccte.cct_dts_src_created as dte_entry,
            ccte.cct_state as status,
            ccte.bln_billable
        from {{ ref("dim_cc_transaction_entry") }} ccte 
        inner join project p on ccte.key_project = p.key
        left join currency_conversion cc_proj on (
                                                    cc_proj.frm_curr = upper(ifnull(nullif(ccte.currency,''),'USD'))
                                                    and cc_proj.to_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                    and cc_proj.date = date(ccte.dts_src_created)
                                                )
        left join currency_conversion cc_usd on (
                                                    cc_usd.frm_curr = upper(ifnull(nullif(p.currency_iso_code,''),'USD'))
                                                    and cc_usd.to_curr = 'USD'
                                                    and cc_usd.date = date(ccte.dts_src_created)
                                                )
        where ccte.bln_billable =true 
    ),
    --*********************************************************************************************************
    -- Time entry final query
    activitybyproject_te as (
         select 
            te.key_project,
            te.key_employee,
            'TIMESHEET' as activity_type, 
            te.key_task,
            te.key_parent,
            te.employee_name_lf,
            te.employee_name ,
            te.employee_name_lf as employee_name_lf_filter,
            te.employee_name as employee_name_filter,
            te.key as record_id,         
            te.currency_iso_code_project as currency_iso_code_project,
            null as currency_iso_code_base,
            te.currency_iso_code as currency_iso_code,
            te.dte_entry,
            te.qty,
            te.task_name,
            te.notes, 
            te.rate,
            te.rate_project,
            te.rate_project_usd,
            te.cost,
            te.cost_project,
            te.cost_project_usd,
            te.status,
            te.bln_billable,
            te.sfc_status
         from timesheet_entry te  
    ),
    --*********************************************************************************************************
    -- Expense item final grouped query
    activitybyproject_ei as ( 
         select
            ei.key_project, 
            ei.key_employee,
            'EXPENSE' as activity_type, 
            null as key_task,  
            ei.key_expense as key_parent, 
            ei.employee_name_lf,
            ei.employee_name,
            ei.employee_name_lf_filter,
            ei.employee_name_filter,
            ei.record_id, 
            ei.currency_iso_code_project,
            ei.currency_iso_code_base,
            null as currency_iso_code,
            ei.dte_entry,
            1 as qty, 
            'EXPENSE' as task_name, 
            null as notes,
            sum(ei.rate) as rate,
            sum(ei.rate_project) as rate_project,
            sum(ei.rate_project_usd) as rate_project_usd,
            sum(ei.cost) as cost,
            sum(ei.cost_project) as cost_project,
            sum(ei.cost_project_usd) as cost_project_usd,
            ei.status,
            ei.bln_billable,
            'N/A' as sfc_status,
         from expense_item ei 
         group by all
    ),
    
    --*********************************************************************************************************
    -- AP Bill Item final grouped query
    activitybyproject_ap as (
         select
            apbi.key_project, 
            apbi.key_employee as key_employee,
            'AP' as activity_type, 
            null as key_task,  
            apbi.key_ap_bill as key_parent, 
            apbi.record_id as employee_name_lf,        
            apbi.record_id as employee_name,
            null as employee_name_lf_filter,
            null as employee_name_filter,
            apbi.record_id, 
            apbi.currency_iso_code_project,
            apbi.currency_iso_code_base,
            null as currency_iso_code,
            apbi.dte_entry,
            1 as qty, 
            'AP' as task_name, 
            null as notes,
            sum(apbi.rate) as rate,
            sum(apbi.rate_project) as rate_project,
            sum(apbi.rate_project_usd) as rate_project_usd,
            sum(apbi.cost) as cost,
            sum(apbi.cost_project) as cost_project,
            sum(apbi.cost_project_usd) as cost_project_usd,
            apbi.status as status,
            apbi.bln_billable as bln_billable,
            'N/A' as sfc_status,
         from ap_bill_item apbi
         group by all
    ),
    
    --*********************************************************************************************************
    -- Credit Card Transaction item final grouped query
    activitybyproject_cct as (
         select
            ccte.key_project, 
            ccte.key_employee as key_employee,
            'EXPENSE - CC' as activity_type, 
            null as key_task,  
            ccte.key_ccte as key_parent, 
            ccte.record_id as employee_name_lf,        
            ccte.record_id as employee_name,
            null as employee_name_lf_filter,
            null as employee_name_filter,
            ccte.record_id, 
            ccte.currency_iso_code_project,
            ccte.currency_iso_code_base,
            null as currency_iso_code,
            ccte.dte_entry,
            1 as qty, 
            'EXPENSE - CC' as task_name, 
            null as notes,
            sum(ccte.rate) as rate,
            sum(ccte.rate_project) as rate_project,
            sum(ccte.rate_project_usd) as rate_project_usd,
            sum(ccte.cost) as cost,
            sum(ccte.cost_project) as cost_project,
            sum(ccte.cost_project_usd) as cost_project_usd,
            ccte.status as status,
            ccte.bln_billable as bln_billable,
            'N/A' as sfc_status,
         from ccte_entry ccte
         group by all
    ),
   
    --*********************************************************************************************************
    -- Union all the final queries together in 1 data set...
    final as (
        select 
            -- Final dataset fields combined
            f.*, 
            
            --project fields
            p.key_location, 
            p.key_practice, 
            p.key_practice_area,
            p.location_id_intacct,
            p.project_id,
            p.location_name,
            p.group_name,
            p.entity_name,
            p.practice_name,
            p.project_manager_name,
            p.project_manager_name_lf,
            p.project_manager_email,
            p.client_site_id,
            p.client_manager_id,
            p.client_manager_name,
            p.client_manager_name_lf,
            p.client_manager_email,
            p.assistant_project_manager_id,
            p.assistant_project_manager_name,
            p.assistant_project_manager_name_lf,
            p.assistant_project_manager_email,
            p.project_name,
            p.project_status,
            p.practice_area_name,
            p.department_name,
            p.customer_id,
            p.customer_name,
            p.practice_id_intacct,
            p.billing_type,
            p.root_parent_name,
            p.amt_po,
            p.amt_po_usd,
    
            --employee fields
            e.ukg_employee_number,
            e.email_address_work,
    
            --account fields
            a.key as key_account, 
            a.account_name, 
            a.key_top_level_parent_account, 
            a.top_level_parent_account_name 
        from (
             select * from activitybyproject_te
             union(select * from activitybyproject_ei)
             union(select * from activitybyproject_ap)
             union(select * from activitybyproject_cct)
        ) as f
        left join project p on f.key_project = p.key
        left join account a on p.account_id = a.key
        left join employee e on f.key_employee = e.key
    )

--*********************************************************************************************************
--Results Query to table
select 
     current_timestamp as dts_created_at,
     'activity_by_project' as created_by,
     current_timestamp as dts_updated_at,
     'activity_by_project' as updated_by,
     key_project,
     coalesce(key_parent,'') as key_parent,
     coalesce(activity_type,'') as activity_type,
     coalesce(key_task,'') as key_task,
     key_location,
     key_practice,
     key_practice_area,
     key_top_level_parent_account,
     key_account,
     coalesce(location_id_intacct,'') as location_id_intacct,
     coalesce(project_id,'') as project_id,
     coalesce(account_name,'') as account_name,
     coalesce(top_level_parent_account_name,'') as top_level_parent_account_name,
     coalesce(location_name,'') as location_name,
     coalesce(group_name,'') as group_name,
     coalesce(entity_name,'') as entity_name,
     coalesce(practice_name,'') as practice_name,
     coalesce(project_manager_name,'') as project_manager_name,
     coalesce(project_manager_name_lf,'') as project_manager_name_lf,
     coalesce(project_manager_email,'') as project_manager_email,
     coalesce(client_site_id,'') as client_site_id,
     coalesce(client_manager_id,'') as client_manager_id, 
     coalesce(client_manager_name,'') as client_manager_name,
     coalesce(client_manager_name_lf,'') as client_manager_name_lf,
     coalesce(client_manager_email,'') as client_manager_email,
     coalesce(assistant_project_manager_id,'') as assistant_project_manager_id, 
     coalesce(assistant_project_manager_name,'') as assistant_project_manager_name,
     coalesce(assistant_project_manager_name_lf,'') as assistant_project_manager_name_lf,
     coalesce(assistant_project_manager_email,'') as assistant_project_manager_email,
     coalesce(ukg_employee_number,'') as ukg_employee_number,
     coalesce(email_address_work,'') as email_address_work,
     coalesce(employee_name_lf,'') as employee_name_lf,
     coalesce(employee_name,'') as employee_name,
     coalesce(employee_name_lf_filter,'') as employee_name_lf_filter,
     coalesce(employee_name_filter,'') as employee_name_filter,
     coalesce(record_id,'') as record_id, 
     coalesce(currency_iso_code_project,'') as currency_iso_code_project,
     coalesce(currency_iso_code,'') as currency_iso_code,
     coalesce(project_name,'') as project_name,
     coalesce(project_status,'') as project_status,
     coalesce(practice_area_name,'') as practice_area_name,
     coalesce(department_name,'') as department_name,
     dte_entry,
     cast(round(qty,2) as number(38,2)) as qty,
     coalesce(task_name,'') as task_name,
     coalesce(customer_id,'') as customer_id ,
     coalesce(customer_name,'') as customer_name,
     coalesce(practice_id_intacct,'') as practice_id_intacct,
     coalesce(billing_type,'') as billing_type,
     coalesce(root_parent_name,'') as root_parent_name,
     coalesce(notes,'') as notes,
     cast(ifnull(amt_po, 0) as number(38,2)) as amt_po,
     cast(ifnull(amt_po_usd, 0) as number(38,2)) as amt_po_usd, 
     cast(ifnull(rate, 0) as number(38,2)) as rate,
     cast(ifnull(rate_project, 0) as number(38,2)) as rate_project, 
     cast(ifnull(rate_project_usd, 0) as number(38,2)) as rate_project_usd,  
     cast(ifnull(cost, 0) as number(38,2)) as cost,
     cast(ifnull(cost_project, 0) as number(38,2)) as cost_project, 
     cast(ifnull(cost_project_usd, 0) as number(38,2)) as cost_project_usd,
     status,
     ifnull(bln_billable, false) as bln_billable,
     sfc_status
from final