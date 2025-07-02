{{
    config(
        alias="backlog",
        materialized="table",
        schema="dataconsumption"
    )
}}

with te as
(select key_project,project_id,project_name, location_id_intacct, location_name, project_manager_name, project_manager_email, project_manager_personal_email, department_name, currency_iso_code, base_currency, currency_code, 
  max(dte_entry) as dte_entry, customer_id , customer_name,  amt_po , amt_po_usd ,   SUM(cost) as total_labour, SUM(cost_project) as total_labour_project, sum(cost_project_usd) as total_labour_usd
from {{ ref('rpt_activity_by_project') }}  where task_name not in ('AP', 'EXPENSE', 'EXPENSE - CC') 
group by all
),
 exp as 
(select key_project,project_id,project_name, location_id_intacct, location_name, project_manager_name, project_manager_email, project_manager_personal_email, department_name, currency_iso_code,
  max(dte_entry) as dte_entry, customer_id , customer_name,  amt_po , amt_po_usd ,   SUM(cost) as total_expense, SUM(cost_project) as total_expense_project, sum(cost_project_usd) as total_expense_usd
from {{ ref('rpt_activity_by_project') }} where task_name  in ('EXPENSE') 
group by all
),
 ap as 
(select key_project,project_id,project_name, location_id_intacct, location_name, project_manager_name, project_manager_email, project_manager_personal_email, department_name, currency_iso_code,
 max(dte_entry) as dte_entry, customer_id , customer_name,  amt_po , amt_po_usd ,   SUM(cost) as total_ap, SUM(cost_project) as total_ap_project, sum(cost_project_usd) as total_ap_usd 
from {{ ref('rpt_activity_by_project') }} where task_name  in ('AP') 
group by all
),
expense_cc as 
(select key_project,project_id,project_name, location_id_intacct, location_name, project_manager_name, project_manager_email, project_manager_personal_email, department_name, currency_iso_code,
 max(dte_entry) as dte_entry, customer_id , customer_name,  amt_po , amt_po_usd ,   SUM(cost) as total_ap, SUM(cost_project) as total_ap_project, sum(cost_project_usd) as total_ap_usd 
from {{ ref('rpt_activity_by_project') }} where task_name  in ('EXPENSE - CC') 
group by all
)
, project_join as 
(select te.key_project,te.project_id,te.project_name, te.location_id_intacct, te.location_name, te.project_manager_name, te.project_manager_email, te.project_manager_personal_email, te.currency_iso_code,
 te.dte_entry, te.customer_id , te.customer_name,  te.amt_po , te.amt_po_usd , total_labour, total_labour_project,
 total_labour_usd, coalesce(total_expense,0) as total_expense, coalesce(total_expense_project,0) as total_expense_project,
  coalesce(total_expense_usd,0) as total_expense_usd,
  coalesce(total_ap,0) as total_ap, coalesce(total_ap_project,0) as total_ap_project, coalesce(total_ap_usd,0) as total_ap_usd,
  coalesce(total_labour_project +total_expense_project +total_ap_project,0) as total_worked_project ,
  coalesce(total_labour_usd +total_expense_usd +total_ap_usd,0) as total_worked_usd,
  coalesce(te.amt_po - total_worked_project,0) as total_remaining_project,
  coalesce( te.amt_po_usd - total_worked_usd,0) as total_remaining_usd,
  coalesce( current_date() - date(te.dte_entry),0) as age,
   case when age >90 then 0.1 
   when age > 60 then 0.2
   when age > 30 then 0.4
   when age > 14 then 0.8
   when age > 7 then 0.9 else 1 end as blprob,
  coalesce(total_remaining_project * blprob,0) as backlog_project,
  coalesce(total_remaining_usd * blprob,0) as backlog_usd 
  from te 
  left join exp on te.key_project = exp.key_project
  left join ap on te.key_project = ap.key_project
  
  )
select 
    current_timestamp as dts_created_at,
    'activity_by_project' as created_by,
    current_timestamp as dts_updated_at,
    'activity_by_project' as updated_by,
* from project_join