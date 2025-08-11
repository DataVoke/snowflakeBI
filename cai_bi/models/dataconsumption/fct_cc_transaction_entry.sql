{{ config(
    materialized='table',
    schema='dataconsumption',
    alias="cc_transaction_entry"
) }}

with
cc_transaction_entry as (
    select * 
    from {{ ref('cc_transaction_entry') }} 
    where src_sys_key = 'int' and bln_line_item = true
),
cc_transaction as (
    select * 
    from {{ ref('cc_transaction') }} 
),
portal_departments as (
    select * 
    from {{ source('portal', 'departments') }}
),
portal_locations as (
    select * 
    from {{ source('portal', 'locations') }} 
    where id != '55-1'
),
portal_entities as (
    select * 
    from {{ source('portal', 'entities') }}
),
locations_intacct as (
    select * 
    from {{ source('sage_intacct', 'location') }} 
),
project as (
    select * 
    from {{ ref('project') }}  
    where src_sys_key = 'int'
),
employee_int as (
    select * 
    from {{ ref('employee') }}  
    where src_sys_key = 'int'
),
employee_ukg as (
    select * 
    from {{ ref('employee') }}  
    where src_sys_key = 'ukg'
),

final as (
    select 
        cte.key,
        cte.hash_key,

        -- keys
        por_loc.record_id as key_location,
        project.key as key_project,
        por_dep.record_id as key_department,
        por_ent.record_id as key_entity,
        employee_ukg.key as key_employee,

        -- display names
        por_dep.display_name as department_name,
        por_loc.display_name as location_name,
        por_ent.display_name as entity_name,
        employee_ukg.display_name as employee_name,
        employee_ukg.display_name_lf as employee_name_lf,
        project.project_id,
        project.project_name,

        cte.key_cc_transaction,
        cte.hash_key_cc_transaction,
        cte.hash_key_project,
        cte.hash_key_entity,
        cte.hash_key_location,
        cte.account_key,
        cte.base_location_id,
        cte.customer_dim_key,
        cte.customer_id,
        cte.department_id,
        cte.employee_id,
        ifnull(nullif(locations_intacct.parentid,''),cte.location_id) as entity_id,
        cte.exchange_rate_type_id,
        cte.gl_dim_vat_code,
        cte.item_dim_key,
        cte.item_id,
        cte.location_id,
        cte.src_created_by_id,
        cte.src_modified_by_id,
        cte.vendor_dim_key,
        cte.vendor_id,
        cte.account_title,
        cte.amt,
        cte.amt_total_expensed,
        cte.amt_total_paid,
        cte.amt_total_selected,
        cte.amt_trx,
        cte.amt_trx_total_paid,
        cte.amt_trx_total_selected,
        cte.base_currency,
        cte.bln_billable,
        cte.bln_billed,
        cte.bln_is_tax,
        cte.bln_line_item,
        cte.cc_txn_payee,
        cte.currency,
        cte.customer_name,
        cte.description,
        cte.dte_exchange_rate,
        cte.dts_src_created,
        cte.dts_src_modified,
        cte.exchange_rate,
        cte.financial_entity,
        cte.item_name,
        cte.line_no,
        cte.record_type,
        cte.record_url,
        cte.status,
        cte.vendor_name,
        cct.key AS cct_key,
        cct.hash_key AS cct_hash_key,
        cct.key_entity AS cct_key_entity,
        cct.hash_key_entity AS cct_hash_key_entity,
        cct.exchange_rate_type_id AS cct_exchange_rate_type_id,
        cct.entity_id AS cct_entity_id,
        cct.pr_batch_key AS cct_pr_batch_key,
        cct.record_id AS cct_record_id,
        cct.src_created_by_id AS cct_src_created_by_id,
        cct.src_modified_by_id AS cct_src_modified_by_id,
        cct.sup_doc_id AS cct_sup_doc_id,
        cct.amt_total_due AS cct_amt_total_due,
        cct.amt_total_entered AS cct_amt_total_entered,
        cct.amt_total_paid AS cct_amt_total_paid,
        cct.amt_total_selected AS cct_amt_total_selected,
        cct.amt_trx_total_due AS cct_amt_trx_total_due,
        cct.amt_trx_total_entered AS cct_amt_trx_total_entered,
        cct.amt_trx_total_paid AS cct_amt_trx_total_paid,
        cct.amt_trx_total_selected AS cct_amt_trx_total_selected,
        cct.base_currency AS cct_base_currency,
        cct.bln_cleared AS cct_bln_cleared,
        cct.bln_inclusive_tax AS cct_bln_inclusive_tax,
        cct.brex_expense_url AS cct_brex_expense_url,
        cct.brex_receipt_url AS cct_brex_receipt_url,
        cct.currency AS cct_currency,
        cct.description AS cct_description,
        cct.description_2 AS cct_description_2,
        cct.dte_exchange_rate AS cct_dte_exchange_rate,
        cct.dts_au_when_created AS cct_dts_au_when_created,
        cct.dts_src_created AS cct_dts_src_created,
        cct.dts_when_paid AS cct_dts_when_paid,
        cct.entity_name AS cct_entity_name,
        cct.exchange_rate AS cct_exchange_rate,
        cct.financial_entity AS cct_financial_entity,
        cct.pr_batch_open AS cct_pr_batch_open,
        cct.raw_state AS cct_raw_state,
        cct.record_type AS cct_record_type,
        cct.record_url AS cct_record_url,
        cct.state AS cct_state
    from cc_transaction_entry cte
    left join cc_transaction cct on cte.key_cc_transaction = cct.key
    left join portal_departments por_dep on cte.department_id = por_dep.intacct_id
    left join portal_locations por_loc on por_loc.intacct_id = cte.location_id
    --left join portal_entities por_ent on por_loc.entity_id = por_ent.id
    left join locations_intacct on cte.location_id = locations_intacct.locationid
    left join portal_entities por_ent on coalesce(locations_intacct.parentkey,cte.BASE_LOCATION_ID) = por_ent.id
    left join project on cte.key_project = project.key
    left join employee_int on cte.employee_id = employee_int.intacct_employee_id
    left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link
    where cct.key is not null
)

select  current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by, * from final
