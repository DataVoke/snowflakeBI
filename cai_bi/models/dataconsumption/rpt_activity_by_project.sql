{{
    config(
        alias="activity_by_project",
        materialized="table",
        schema="dataconsumption"
    )
}}

with
project as ( select * from {{ ref('dim_project') }} where dte_src_start is not null),
ap_bill_item as ( select * from {{ ref('fct_ap_bill_item') }} where bln_billable =true  ),
timesheet_entry as (select * from {{ ref('fct_timesheet_entry') }} ),
employee as (select * from {{ ref('dim_employee') }}  ),
expense_item as (select *  from {{ ref('fct_expense_item') }} where bln_billable =true ),
ccte_entry as (select * from {{ ref('fct_cc_transaction_entry') }} where bln_billable =true ),
forex_filtered as ( select * from {{ ref('ref_fx_rates_timeseries')}} where to_curr = 'USD' ),
forex_projectcurr as ( select * from {{ ref('ref_fx_rates_timeseries')}} ),
activitybyproject_te as
(
        select 
        p.key as key_project,
        te.key_timesheet as key_parent,
        p.location_id_intacct,
        p.project_id,
        p.location_name ,
        p.group_name ,
        p.entity_name,
        p.practice_name,
        p.project_manager_name,
        p.project_manager_name_lf,
        p.email_address_work as project_manager_email,
        p.email_address_personal as project_manager_personal_email,
        p.client_site_id,
        p.client_manager_id,
        p.client_manager_name,
        p.client_manager_name_lf,
        p.client_manager_email,
        p.assistant_project_manager_id,
        p.assistant_project_manager_name,
        p.assistant_project_manager_name_lf,
        p.assistant_project_manager_email,
        te_e.ukg_employee_number,
        te_e.email_address_work,
        te.employee_name_lf,
        te.employee_name ,
        p.currency_iso_code,
        null as base_currency,
        null as currency_code,
        --0 as curr_ind,
        p.project_name,
        p.project_status,
        p.practice_area_name,
        p.department_name,
        te.dte_entry,
        te.qty,
        te.task_name,
        p.customer_id ,
        p.customer_name ,
        p.practice_id_intacct,
        p.billing_type,
        p.root_parent_name,
        te.notes,
        --coalesce(ex.fx_rate_div,1) as rate_div,
        --coalesce(ex.fx_rate_mul,1) as rate_mul,
        --null as pcurr_rate_div,
        --null as pcurr_rate_mul,
        p.amt_po,
        p.amt_po_usd, 
        coalesce(round(te.bill_rate,2),0) as rate,
        coalesce(round(te.bill_rate,2),0) as rate_project,
        round(case when rate_div >={{ var('rate_threshold') }} then rate_project/rate_div else rate_project*rate_mul end ,2) as rate_project_usd,
        round(coalesce( rate * te.qty,0),2) as cost,
        round(coalesce( rate * te.qty,0),2) as cost_project,
        round(case when rate_div >={{ var('rate_threshold') }} then cost_project/rate_div else cost_project*rate_mul end ,2) as cost_project_usd
        from timesheet_entry te  
        inner join project p on te.hash_key_project = p.hash_key
        left join employee te_e on te.hash_key_employee = te_e.hash_key
        left join  forex_filtered ex on (p.currency_iso_code = ex.frm_curr )
            and ex.date = te.dte_entry
             --qualify row_number() over ( partition by te.key  order by ex.date desc ) =1
),

expi_with_project as 
( select  p.key as key_project, ei.key key_expense_item, ei.key_expense as key_expense, p.location_id_intacct, p.project_id, p.location_name, p.group_name ,p.entity_name, p.practice_name, 
p.project_manager_name,
p.project_manager_name_lf,
        p.email_address_work as project_manager_email,
        p.email_address_personal as project_manager_personal_email,
        p.client_site_id,
        p.client_manager_id,
        p.client_manager_name,
        p.client_manager_name_lf,
        p.client_manager_email,
        p.assistant_project_manager_id,
        p.assistant_project_manager_name,
        p.assistant_project_manager_name_lf,
        p.assistant_project_manager_email,
        ei_e.ukg_employee_number as ukg_employee_number, ei_e.email_address_work as email_address_work,
        case when ei.employee_name_lf is null or ei.employee_name_lf ='' then exp_record_id 
        else ei.employee_name_lf ||' - ' || exp_record_id end as employee_name_lf,        
        case when ei.employee_name is null or ei.employee_name ='' then exp_record_id 
        else ei.employee_name ||' - ' || exp_record_id end as employee_name,
        p.currency_iso_code, ei.exp_currency , ei.org_currency ,
        case
              when ei.org_currency = p.currency_iso_code then 1
              when ei.exp_currency =  p.currency_iso_code  then 2
              else 3
        end as curr_ind, p.amt_po, p.amt_po_usd, coalesce(round(ei.amt,2),0) as amt, coalesce(round(ei.amt_org,2),0) as amt_org, p.project_name, p.project_status, p.practice_area_name, p.department_name,
        coalesce( ei.dte_org_exchrate,ei.dte_entry,ei.exp_dte_when_posted) as dte_exch_rate, ei.exp_dte_when_posted as dte_entry,
        1 as qty, 'EXPENSE' as task_name, p.customer_id , p.customer_name , p.practice_id_intacct, p.billing_type,p.root_parent_name, null as notes
        from expense_item ei 
          inner join project p on ei.hash_key_project = p.hash_key
          left join employee ei_e on ei.hash_key_employee = ei_e.hash_key

),
exchange_matched_ei as 
(  select key_project,key_expense_item, key_expense, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,
        project_manager_email,project_manager_personal_email,
        client_site_id,
        client_manager_id,
        client_manager_name,client_manager_name_lf,client_manager_email, assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,
currency_iso_code,exp_currency,org_currency ,curr_ind,amt_po, amt_po_usd, amt, amt_org,project_name,project_status,practice_area_name,department_name,dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
        coalesce(ex.fx_rate_div,1) as rate_div,
        coalesce(ex.fx_rate_mul,1) as rate_mul, 
        ex.date
          from expi_with_project ei
          left join forex_filtered ex
            on (ei.currency_iso_code = ex.frm_curr )
            and ex.date = ei.dte_exch_rate
            -- qualify row_number() over ( partition by ei.key_expense_item  order by ex.date desc ) =1
),
 exchange_matched_projcurr_ei as 
 ( select key_project, key_expense_item,key_expense, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name, project_manager_name_lf, client_site_id,
 project_manager_email,project_manager_personal_email, client_manager_id,
        client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,
 exp_currency,org_currency,curr_ind,project_name,project_status,practice_area_name,department_name,dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
        exm.rate_div,exm.rate_mul, coalesce(ex_pcurr.fx_rate_div,1) as pcurr_rate_div, coalesce(ex_pcurr.fx_rate_mul,1) as pcurr_rate_mul,  exm.date, ex_pcurr.date as pcurr_date,
            amt_po, amt_po_usd,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else amt_org
        end,2) as rate,    
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else ( case when rate_div >={{ var('rate_threshold') }} then amt_org/pcurr_rate_div else amt_org * pcurr_rate_mul end ) 
        end,2) as rate_project,
        round(case when rate_div >={{ var('rate_threshold') }} then rate_project/rate_div else rate_project*rate_mul end ,2) as rate_project_usd,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else amt_org
        end,2) as cost,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else ( case when rate_div >={{ var('rate_threshold') }} then amt_org/pcurr_rate_div else amt_org * pcurr_rate_mul end) 
        end,2) as cost_project,        
        round(case when rate_div >={{ var('rate_threshold') }} then cost_project/rate_div else cost_project*rate_mul end ,2) as cost_project_usd
          from exchange_matched_ei exm
          left join forex_projectcurr ex_pcurr
            on (exm.currency_iso_code = ex_pcurr.to_curr  )
            and ( ex_pcurr.frm_curr = exm.org_currency)
            and ex_pcurr.date = exm.dte_exch_rate
            -- qualify row_number() over ( partition by exm.key_expense_item  order by ex_pcurr.date desc ) =1
),
agg_by_keyei as (     
        select key_project,key_expense,  location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,client_site_id,project_manager_email,project_manager_personal_email,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,
        currency_iso_code,exp_currency as base_currency,org_currency as currency_code,
        project_name,project_status,practice_area_name,department_name,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            amt_po,amt_po_usd , sum(rate) as rate,  sum(rate_project) as rate_project ,sum(rate_project_usd) as rate_project_usd, sum(cost) as cost ,sum(cost_project) as cost_project, sum(cost_project_usd) as cost_project_usd
        from exchange_matched_projcurr_ei group by all   
),
activitybyproject_ei as (     
        select key_project,key_expense as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id, 
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email, ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,
        project_name,project_status,practice_area_name,department_name,dte_entry ,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            amt_po,amt_po_usd ,  rate ,rate_project, rate_project_usd,  cost ,cost_project, cost_project_usd
        from agg_by_keyei   )
        ,
apbi_with_project as  
( select  p.key as key_project,apbi.key as key_api, apbi.key_ap_bill,  p.location_id_intacct, p.project_id, p.location_name, p.group_name ,p.entity_name, p.practice_name, p.project_manager_name, p.project_manager_name_lf,
        p.email_address_work as project_manager_email,
        p.email_address_personal as project_manager_personal_email,
        p.client_site_id, p.client_manager_id, p.client_manager_name,p.client_manager_name_lf,p.client_manager_email,
        p.assistant_project_manager_id, p.assistant_project_manager_name, p.assistant_project_manager_name_lf, p.assistant_project_manager_email,
        '' as ukg_employee_number, '' as email_address_work, ap_record_id as employee_name_lf,ap_record_id as employee_name , p.currency_iso_code,apbi.base_currency, apbi.currency_code,
        case
              when apbi.base_currency = p.currency_iso_code then 1
              when apbi.currency_code = p.currency_iso_code then 2
              else 3
        end as curr_ind, amt_po,amt_po_usd, coalesce(round(apbi.amt,2),0) as amt, coalesce(round(apbi.amt_trx,2),0) as amt_trx, p.project_name, p.project_status, p.practice_area_name, p.department_name,
        coalesce( apbi.dte_exch_rate,apbi.dte_entry,apbi.ap_dte_when_posted) as dte_exch_rate, apbi.ap_dte_when_posted as dte_entry,
        1 as qty, 'AP' as task_name, p.customer_id , p.customer_name , p.practice_id_intacct, p.billing_type,p.root_parent_name, null as notes
        from ap_bill_item apbi
          inner join project p on apbi.hash_key_project = p.hash_key 
),
exchange_matched_api as 
(  select key_project,key_api, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email, ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,amt_po,amt_po_usd, amt, amt_trx,project_name,project_status,practice_area_name,department_name,
        dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
        coalesce(ex.fx_rate_div,1) as rate_div,
        coalesce(ex.fx_rate_mul,1) as rate_mul, 
        ex.date
          from apbi_with_project apbi
          left join forex_filtered ex
            on (apbi.currency_iso_code = ex.frm_curr )
            and ex.date = apbi.dte_exch_rate
            -- qualify row_number() over ( partition by apbi.key_api  order by ex.date desc ) =1
),
exchange_matched_projcurr_api as 
( select 
            key_project,key_api, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,department_name,dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            exm.rate_div,exm.rate_mul, coalesce(ex_pcurr.fx_rate_div,1) as pcurr_rate_div, coalesce(ex_pcurr.fx_rate_mul,1) as pcurr_rate_mul,  exm.date, ex_pcurr.date as pcurr_date,
            amt_po, amt_po_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx
        end,2) as rate,            
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else ( case when rate_div >={{ var('rate_threshold') }} then amt_trx/pcurr_rate_div else amt_trx * pcurr_rate_mul end)
        end,2) as rate_project,
        round(case when rate_div >={{ var('rate_threshold') }} then rate_project/rate_div else rate_project*rate_mul end ,2) as rate_project_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx
        end,2) as cost,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else ( case when rate_div >={{ var('rate_threshold') }} then  amt_trx/pcurr_rate_div else amt_trx * pcurr_rate_mul end)
        end,2) as cost_project,        
        round(case when rate_div >={{ var('rate_threshold') }} then cost_project/rate_div else cost_project*rate_mul end ,2) as cost_project_usd
          from exchange_matched_api exm
          left join forex_projectcurr ex_pcurr
            on (exm.currency_iso_code = ex_pcurr.to_curr  )
            and ( ex_pcurr.frm_curr = exm.currency_code)
            and ex_pcurr.date = exm.dte_exch_rate
             --qualify row_number() over ( partition by exm.key_api  order by ex_pcurr.date desc ) =1
),
agg_by_keyapbill as (     
select key_project, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,
        project_name,project_status,practice_area_name,department_name,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            amt_po,amt_po_usd , sum(rate) as rate,  sum(rate_project) as rate_project ,sum(rate_project_usd) as rate_project_usd, sum(cost) as cost ,sum(cost_project) as cost_project, sum(cost_project_usd) as cost_project_usd
        from exchange_matched_projcurr_api group by all   ), 
activitybyproject_ap as 
(select key_project,key_ap_bill as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,project_name,project_status,practice_area_name,department_name,
        dte_entry ,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            amt_po,amt_po_usd ,  rate ,rate_project, rate_project_usd,  cost ,cost_project, cost_project_usd
from agg_by_keyapbill),
--
ccte_with_project as  
( select  p.key as key_project,ccte.key as key_ccte, ccte.key_cc_transaction,  p.location_id_intacct, p.project_id, p.location_name, p.group_name ,p.entity_name, p.practice_name, p.project_manager_name,p.project_manager_name_lf,
        p.email_address_work as project_manager_email,
        p.email_address_personal as project_manager_personal_email,
        p.client_site_id, p.client_manager_id, p.client_manager_name,p.client_manager_name_lf,p.client_manager_email,
        p.assistant_project_manager_id, p.assistant_project_manager_name, p.assistant_project_manager_name_lf, p.assistant_project_manager_email,
        ccte_e.ukg_employee_number as ukg_employee_number, ccte_e.email_address_work as email_address_work,
        case when ccte.employee_name_lf is null or ccte.employee_name_lf ='' then ccte.key 
        else ccte.employee_name_lf ||' - ' || ccte.key end as employee_name_lf,
        case when ccte.employee_name is null or ccte.employee_name ='' then ccte.key 
        else ccte.employee_name ||' - ' || ccte.key end as employee_name,
        p.currency_iso_code,ccte.base_currency, ccte.currency as currency_code,
        case
              when ccte.base_currency = p.currency_iso_code then 1
              when ccte.currency = p.currency_iso_code then 2
              else 3
        end as curr_ind, amt_po,amt_po_usd, coalesce(round(ccte.amt,2),0) as amt, coalesce(round(ccte.amt_trx,2),0) as amt_trx, p.project_name, p.project_status, p.practice_area_name, p.department_name,
         ccte.dts_src_created as dte_exch_rate, ccte.cct_dts_src_created as dte_entry,
        1 as qty, 'EXPENSE - CC' as task_name, p.customer_id , p.customer_name , p.practice_id_intacct, p.billing_type,p.root_parent_name, null as notes
        from ccte_entry ccte
          inner join project p on ccte.hash_key_project = p.hash_key 
          left join employee ccte_e on ccte.hash_key_employee = ccte_e.hash_key
),
exchange_matched_ccte as 
(  select key_project,key_ccte, key_cc_transaction, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,amt_po,amt_po_usd, amt, amt_trx,project_name,project_status,practice_area_name,department_name,
        dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
        coalesce(ex.fx_rate_div,1) as rate_div,
        coalesce(ex.fx_rate_mul,1) as rate_mul, 
        ex.date
          from ccte_with_project ccte
          left join forex_filtered ex
            on (ccte.currency_iso_code = ex.frm_curr )
            and ex.date = ccte.dte_exch_rate
            -- qualify row_number() over ( partition by ccte.key_ccte  order by ex.date desc ) =1
),
exchange_matched_projcurr_ccte as 
( select 
            key_project,key_ccte, key_cc_transaction, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,department_name,
        dte_exch_rate,dte_entry,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            exm.rate_div,exm.rate_mul, coalesce(ex_pcurr.fx_rate_div,1) as pcurr_rate_div, coalesce(ex_pcurr.fx_rate_mul,1) as pcurr_rate_mul,  exm.date, ex_pcurr.date as pcurr_date,
            amt_po, amt_po_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx
        end,2) as rate,            
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else ( case when rate_div >={{ var('rate_threshold') }} then amt_trx/pcurr_rate_div else amt_trx * pcurr_rate_mul end)
        end,2) as rate_project,
        round(case when rate_div >={{ var('rate_threshold') }} then rate_project/rate_div else rate_project * rate_mul end ,2) as rate_project_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx
        end,2) as cost,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else ( case when rate_div >={{ var('rate_threshold') }} then  amt_trx/pcurr_rate_div else amt_trx * pcurr_rate_mul end)
        end,2) as cost_project,        
        round(case when rate_div >={{ var('rate_threshold') }} then cost_project/rate_div else cost_project * rate_mul end ,2) as cost_project_usd
          from exchange_matched_ccte exm
          left join forex_projectcurr ex_pcurr
            on (exm.currency_iso_code = ex_pcurr.to_curr  )
            and ( ex_pcurr.frm_curr = exm.currency_code)
            and ex_pcurr.date = exm.dte_exch_rate
            -- qualify row_number() over ( partition by exm.key_ccte  order by ex_pcurr.date desc ) =1
),
activitybyproject_cct as 
(select key_project,key_cc_transaction as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,project_manager_name_lf,project_manager_email,project_manager_personal_email,client_site_id,
        client_manager_id, client_manager_name,client_manager_name_lf,client_manager_email,assistant_project_manager_id, assistant_project_manager_name, assistant_project_manager_name_lf,
        assistant_project_manager_email,ukg_employee_number,email_address_work,employee_name_lf,employee_name ,currency_iso_code,base_currency,currency_code,project_name,project_status,practice_area_name,department_name,
        dte_entry ,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,root_parent_name,notes,
            amt_po,amt_po_usd ,  rate ,rate_project, rate_project_usd,  cost ,cost_project, cost_project_usd
from exchange_matched_projcurr_ccte), 
final as 
(select   * from activitybyproject_te
union
select    * from activitybyproject_ei
union
select    * from activitybyproject_ap
union
select    * from activitybyproject_cct
)
    select 
    current_timestamp as dts_created_at,
    'activity_by_project' as created_by,
    current_timestamp as dts_updated_at,
    'activity_by_project' as updated_by,
     key_project,
      coalesce(key_parent,'') as key_parent,
     coalesce(location_id_intacct,'') as location_id_intacct,
     coalesce(project_id,'') as project_id,
     coalesce(location_name,'') as location_name,
     coalesce(group_name,'') as group_name,
     coalesce(entity_name,'') as entity_name,
     coalesce(practice_name,'') as practice_name,
     coalesce(project_manager_name,'') as project_manager_name,
     coalesce(project_manager_name_lf,'') as project_manager_name_lf,
     coalesce(project_manager_email,'') as project_manager_email,
     coalesce(project_manager_personal_email,'') as project_manager_personal_email,
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
     coalesce(currency_iso_code,'') as currency_iso_code,
     coalesce(base_currency,'') as base_currency,
     coalesce(currency_code,'') as currency_code,
     coalesce(project_name,'') as project_name,
     coalesce(project_status,'') as project_status,
     coalesce(practice_area_name,'') as practice_area_name,
     coalesce(department_name,'') as department_name,
     dte_entry,
     round(qty,2) as qty,
     coalesce(task_name,'') as task_name,
     coalesce(customer_id,'') as customer_id ,
     coalesce(customer_name,'') as customer_name,
     coalesce(practice_id_intacct,'') as practice_id_intacct,
     coalesce(billing_type,'') as billing_type,
     coalesce(root_parent_name,'') as root_parent_name,
     coalesce(notes,'') as notes,
     amt_po,
     amt_po_usd, 
     rate ,
     rate_project, 
     rate_project_usd,  
     cost ,
     cost_project, 
     cost_project_usd
     from final