{{
    config(
        alias="activity_by_project",
        materialized="table",
        schema="dataconsumption"
    )
}}

with
project as ( select * from {{ ref('dim_project') }} where project_type in ('billable', 'coactive billable') and dte_src_start is not null),
ap_bill_item as ( select * from {{ ref('fct_ap_bill_item') }} where bln_billable =true  ),
timesheet_entry as (select * from {{ ref('fct_timesheet_entry') }} ),
employee as (select * from {{ ref('dim_employee') }}  ),
expense_item as (select *  from {{ ref('fct_expense_item') }} where bln_billable =true ),
forex_filtered as ( select * from {{ ref('ref_forex_metrics')}} where to_curr = 'usd'  ),
forex_projectcurr as ( select * from {{ ref('ref_forex_metrics')}} ),
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
        te_e.ukg_employee_number,
        te.employee_name ,
        p.currency_iso_code,
        null as base_currency,
        null as currency_code,
        0 as curr_ind,
        p.project_name,
        p.project_status,
        p.practice_area_name,
        te.dte_entry,
        te.qty,
        te.task_name,
        te.customer_id ,
        te.customer_name ,
        p.practice_id_intacct,
        p.billing_type,
        te.notes,
        coalesce(ex.close,1) as to_usd_close,
        null as to_pcurr_close,
        p.amt_po,
        round(amt_po/to_usd_close,2) as amt_po_usd, 
        coalesce(round(te.bill_rate,2),0) as bill_rate,
        round(coalesce( te.bill_rate * te.qty,0),2) as cost,
        round(cost/to_usd_close,2) as cost_usd
        from timesheet_entry te  
        inner join project p on te.key_project = p.key
        left join employee te_e on te.key_employee = te_e.key
        left join  forex_filtered ex on (p.currency_iso_code = ex.frm_curr )
            and ex.to_curr = 'usd'
            and ex.run_date <= te.dte_entry
             qualify row_number() over ( partition by te.key  order by ex.run_date desc ) =1
),

expi_with_project as 
( select  p.key as key_project, ei.key key_expense_item, ei.key_expense as key_expense, p.location_id_intacct, p.project_id, p.location_name, p.group_name ,p.entity_name, p.practice_name, p.project_manager_name,
        ei_e.ukg_employee_number as ukg_employee_number,
        ei.employee_name as employee_name , p.currency_iso_code,ei.org_currency, ei.currency currency,
        case
              when ei.org_currency = p.currency_iso_code then 1
              when ei.currency =  p.currency_iso_code  then 2
              else 3
        end as curr_ind, coalesce(round(p.amt_po,2),0) as amt_po, coalesce(round(ei.amt,2),0) as amt, coalesce(round(ei.amt_org,2),0) as amt_org, p.project_name, p.project_status, p.practice_area_name, ei.dts_when_posted,
        1 as qty, 'expense' as task_name, ei.customer_id , ei.customer_name , p.practice_id_intacct, p.billing_type, null as notes
        from expense_item ei 
          inner join project p on ei.key_project = p.key
          left join employee ei_e on ei.key_employee = ei_e.key

),
exchange_matched_ei as 
(  select key_project,key_expense_item, key_expense, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,curr_ind,amt_po, amt, amt_org,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
            coalesce(ex.close,1) as to_usd_close, ex.run_date, ex.currency_pair_name
          from expi_with_project ei
          left join forex_filtered ex
            on (ei.currency_iso_code = ex.frm_curr )
            and ex.to_curr = 'usd'
            and ex.run_date <= ei.dts_when_posted
             qualify row_number() over ( partition by ei.key_expense_item  order by ex.run_date desc ) =1
),
 exchange_matched_projcurr_ei as 
 ( select key_project, key_expense_item,key_expense, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
        exm.currency_pair_name,ex_pcurr.currency_pair_name as currency_pair_name_pcurr,exm.to_usd_close,
            coalesce(ex_pcurr.close,1) as to_pcurr_close, exm.run_date, ex_pcurr.run_date as run_date_pcurr,
            amt_po,
            round(amt_po/to_usd_close,2) as amt_po_usd,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else amt_org/to_pcurr_close
        end,2) as bill_rate,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else amt_org/to_pcurr_close
        end,2) as cost,
        round(cost/to_usd_close,2) as cost_usd
          from exchange_matched_ei exm
          left join forex_projectcurr ex_pcurr
            on (exm.currency_iso_code = ex_pcurr.to_curr  )
            and ( ex_pcurr.frm_curr = exm.org_currency)
            and ex_pcurr.run_date <= exm.dts_when_posted
             qualify row_number() over ( partition by exm.key_expense_item  order by ex_pcurr.run_date desc ) =1
),
agg_by_keyei as (     
        select key_project,key_expense,  location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,to_usd_close,to_pcurr_close,
            amt_po,amt_po_usd , sum(bill_rate) as bill_rate , sum(cost) as cost , sum(cost_usd) as cost_usd
        from exchange_matched_projcurr_ei group by all   
),
activitybyproject_ei as (     
        select key_project,key_expense as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,to_usd_close,to_pcurr_close,
            amt_po,amt_po_usd ,  bill_rate ,  cost , cost_usd
        from agg_by_keyei   )
        ,
apbi_with_project as  
( select  p.key as key_project,apbi.key as key_api, apbi.key_ap_bill,  p.location_id_intacct, p.project_id, p.location_name, p.group_name ,p.entity_name, p.practice_name, p.project_manager_name,
        '' as ukg_employee_number, '' as employee_name , p.currency_iso_code,apbi.base_currency, apbi.currency_code,
        case
              when apbi.base_currency = p.currency_iso_code then 1
              when apbi.currency_code = p.currency_iso_code then 2
              else 3
        end as curr_ind, coalesce(round(p.amt_po,2),0) as amt_po, coalesce(round(apbi.amt,2),0) as amt, coalesce(round(apbi.amt_trx,2),0) as amt_trx, p.project_name, p.project_status, p.practice_area_name, apbi.dte_when_posted,
        1 as qty, 'ap' as task_name, apbi.customer_id , apbi.customer_name , p.practice_id_intacct, p.billing_type, null as notes
        from ap_bill_item apbi
          inner join project p on apbi.key_project = p.key 
),
exchange_matched_api as 
(  select key_project,key_api, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,amt_po, amt, amt_trx,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
            coalesce(ex.close,1) as to_usd_close, ex.run_date, ex.currency_pair_name
          from apbi_with_project apbi
          left join forex_filtered ex
            on (apbi.currency_iso_code = ex.frm_curr )
            and ex.to_curr = 'usd'
            and ex.run_date <= apbi.dte_when_posted
             qualify row_number() over ( partition by apbi.key_api  order by ex.run_date desc ) =1
),
exchange_matched_projcurr_api as 
( select 
            key_project,key_api, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
             exm.currency_pair_name,ex_pcurr.currency_pair_name as currency_pair_name_pcurr,exm.to_usd_close,
            coalesce(ex_pcurr.close,1) as to_pcurr_close, exm.run_date, ex_pcurr.run_date as run_date_pcurr,
            amt_po,
            round(amt_po/to_usd_close,2) as amt_po_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx/to_pcurr_close
        end,2) as bill_rate,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else amt_trx/to_pcurr_close
        end,2) as cost,
        round(cost/to_usd_close,2) as cost_usd
          from exchange_matched_api exm
          left join forex_projectcurr ex_pcurr
            on (exm.currency_iso_code = ex_pcurr.to_curr  )
            and ( ex_pcurr.frm_curr = exm.currency_code)
            and ex_pcurr.run_date <= exm.dte_when_posted
             qualify row_number() over ( partition by exm.key_api  order by ex_pcurr.run_date desc ) =1
),
agg_by_keyapbill as (     
select key_project, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,to_usd_close,to_pcurr_close,
            amt_po,amt_po_usd , sum(bill_rate) as bill_rate , sum(cost) as cost , sum(cost_usd) as cost_usd
        from exchange_matched_projcurr_api group by all   ), 
activitybyproject_ap as 
(select key_project,key_ap_bill as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,to_usd_close usd_exch,to_pcurr_close pcurr_exch,
            amt_po,amt_po_usd ,  bill_rate ,  cost ,  cost_usd
from agg_by_keyapbill)
select         current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by, * from activitybyproject_te
union
select         current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by, * from activitybyproject_ei
union
select         current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by, * from activitybyproject_ap