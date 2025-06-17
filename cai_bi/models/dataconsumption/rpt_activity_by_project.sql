{{
    config(
        alias="activity_by_project",
        materialized="table",
        schema="dataconsumption"
    )
}}

with 
    project as ( select * from {{ ref('dim_project') }} where project_type = 'billable' and dte_src_start is not null  ),
    timesheet_entry as (select * from {{ ref('fct_timesheet_entry') }} ), 
    employee as (select * from {{ ref('dim_employee') }}),
    exchange as (select * from {{ source('forex_tracking_currency_exchange_rates_by_day', 'forex_metrics')}} where substr(currency_pair_name,0,3) ='usd' ),
    activitybyproject as
    (
        select 
            p.key as key_project,
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
            p.invoice_currency,
            p.amt_po, 
            round((p.amt_po/case when substr(currency_pair_name,5) is null then 1 else close end),2) as amt_po_usd,
            close as closing_exch_rate,
            p.project_name,
            p.project_status,
            te.practice_area_name,
            te.bill_rate,
            te.dte_entry,
            te.qty,
            te.task_name,
            p.customer_id,
            p.customer_name,
            te.customer_id as customer_id_timeentry,
            te.customer_name as customer_name_timeentry,
            p.practice_id_intacct,
            p.billing_type,
            te.notes,
            round(coalesce( te.bill_rate * te.qty,0),2) as cost,
            round((cost/case when substr(currency_pair_name,5) is null then 1 else close end),2) as cost_usd
        from project p
        left join timesheet_entry te on te.key_project = p.key
        left join employee te_e on te.key_employee = te_e.key
        left join exchange ex on case when substr(currency_pair_name,5) is null then 'usd' else substr(currency_pair_name,5) end = p.currency_iso_code and te.dte_entry = ex.run_date
    )
select * from activitybyproject