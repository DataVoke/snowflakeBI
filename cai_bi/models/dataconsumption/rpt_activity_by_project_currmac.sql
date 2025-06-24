{{
    config(
        alias="activity_by_project_currmac",
        materialized="table",
        schema="dataconsumption"
    )
}}

with
project as ( select * from {{ ref('dim_project') }} where lower(project_type) in ('billable', 'coactive billable') and dte_src_start is not null),
ap_bill_item as ( select * from {{ ref('fct_ap_bill_item') }} where bln_billable =true  ),
timesheet_entry as (select * from {{ ref('fct_timesheet_entry') }} ),
employee as (select * from {{ ref('dim_employee') }}  ),
expense_item as (select *  from {{ ref('fct_expense_item') }} where bln_billable =true ),
forex_filtered as ( select * from {{ ref('ref_forex_metrics')}} where lower(to_curr) = 'usd' ),
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
        coalesce(round(p.amt_po,2),0) as amt_po,
        round({{convert_currency_through_usd('amt_po', 'p.currency_iso_code', "'USD'", 'te.dte_entry', ref('ref_forex_metrics'))}},2) as amt_po_usd,
        --coalesce(round(te.bill_rate,2),0) as bill_rate,
        --round(coalesce( te.bill_rate * te.qty,0),2) as cost,
        --round({{convert_currency_through_usd('cost', 'p.currency_iso_code', "'USD'", 'te.dte_entry', ref('ref_forex_metrics'))}},2) as cost_usd
        from timesheet_entry te  
        inner join project p on te.key_project = p.key
        left join employee te_e on te.key_employee = te_e.key
)
/*
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
 exchange_matched_projcurr_ei as 
 ( select key_project, key_expense_item,key_expense, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,
 curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
        amt_po,
        round(convert_currency_through_usd(amt_po, currency_iso_code, 'USD', exm.dts_when_posted, {{ ref('ref_forex_metrics')}}),2) as amt_po_usd,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else round(convert_currency_through_usd(amt_org, org_currency, currency_iso_code, exm.dts_when_posted, {{ ref('ref_forex_metrics')}}),2) 
        end,2) as bill_rate,
        round(case when curr_ind =1 then amt_org 
             when curr_ind =2 then amt
        else round(convert_currency_through_usd(amt_org, org_currency, currency_iso_code, exm.dts_when_posted, {{ ref('ref_forex_metrics')}}),2) 
        end,2) as cost,
        round(convert_currency_through_usd(cost, currency_iso_code, 'USD', exm.dts_when_posted, {{ ref('ref_forex_metrics')}}),2) as cost_usd
          from expi_with_project exm
),
agg_by_keyei as (     
        select key_project,key_expense,  location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,
        curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
            amt_po,amt_po_usd , sum(bill_rate) as bill_rate , sum(cost) as cost , sum(cost_usd) as cost_usd
        from exchange_matched_projcurr_ei group by all   
),
activitybyproject_ei as (     
        select key_project,key_expense as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,org_currency,currency ,curr_ind,project_name,project_status,practice_area_name,dts_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
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
exchange_matched_projcurr_api as 
( select 
            key_project,key_api, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,
            ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,
            qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,amt_po,
            round(convert_currency_through_usd(amt_po, currency_iso_code, 'USD', exm.dte_when_posted, {{ ref('ref_forex_metrics')}}),2) as amt_po_usd,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else round(convert_currency_through_usd(amt_trx, currency_code, currency_iso_code, exm.dte_when_posted, {{ ref('ref_forex_metrics')}}),2) 
        end,2) as bill_rate,
        round(case when curr_ind =1 then amt 
             when curr_ind =2 then amt_trx
        else round(convert_currency_through_usd(amt_trx, currency_code, currency_iso_code, exm.dte_when_posted, {{ ref('ref_forex_metrics')}}),2) 
        end,2) as cost,
         round(convert_currency_through_usd(cost, currency_iso_code, 'USD', exm.dte_when_posted, {{ ref('ref_forex_metrics')}}),2) as cost_usd,
          from apbi_with_project exm
),
agg_by_keyapbill as (     
select key_project, key_ap_bill, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
            amt_po,amt_po_usd , sum(bill_rate) as bill_rate , sum(cost) as cost , sum(cost_usd) as cost_usd
        from exchange_matched_projcurr_api group by all   ), 
activitybyproject_ap as 
(select key_project,key_ap_bill as key_parent, location_id_intacct,project_id,location_name,group_name ,entity_name,practice_name,project_manager_name,ukg_employee_number,employee_name ,currency_iso_code,base_currency,currency_code,curr_ind,project_name,project_status,practice_area_name,dte_when_posted,qty,task_name,customer_id ,customer_name ,practice_id_intacct,billing_type,notes,
            amt_po,amt_po_usd ,  bill_rate ,  cost ,  cost_usd
from agg_by_keyapbill) */
select         current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by, * from activitybyproject_te
    /*
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
    */