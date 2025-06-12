{{ config(
    materialized='table',
    schema='dataconsumption',
    alias="ap_bill_item"
) }}

with
ap_bill_item as (
    select * 
    from {{ ref('ap_bill_item') }} 
    where src_sys_key = 'int' and bln_line_item = true
),
ap_bill as (
    select * 
    from {{ ref('ap_bill') }} 
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
        abi.key,
        abi.key_ap_bill,

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
        project.project_id,
        project.project_name,

        -- ap_bill_item fields
        abi.account_key,
        abi.customer_key,
        abi.customer_id,
        abi.department_id,
        abi.detail_key,
        abi.employee_id,
        abi.exch_rate_type_id,
        abi.item_key,
        abi.item_id,
        abi.location_id,
        abi.offset_account_key,
        abi.project_id ap_bill_project_id,
        abi.record_id,
        abi.src_created_by_id,
        abi.src_modified_by_id,
        abi.vendor_key,
        abi.vendor_id,
        abi.dte_exch_rate,
        abi.amt_non_reclaim_vat_base,
        abi.account_no,
        abi.account_title,
        abi.amount,
        abi.amt_base_vat,
        abi.amt_net_of_vat,
        abi.amt_net_of_vat_base,
        abi.amt_non_reclaim_vat,
        abi.amt_reclaim_vat,
        abi.amt_reclaim_vat_base,
        abi.amt_retained,
        abi.amt_reverse_txn_vat,
        abi.amt_reverse_txn_vat_base,
        abi.amt_total_paid,
        abi.amt_trx,
        abi.amt_trx_total_paid,
        abi.basecurr,
        abi.baselocation,
        abi.bln_amt_manual_vat,
        abi.bln_billable,
        abi.bln_billed,
        abi.bln_include_tax_in_asset_cost,
        abi.bln_india_cgst,
        abi.bln_india_igst,
        abi.bln_india_rcm,
        abi.bln_india_sgst,
        abi.bln_line_item,
        abi.bln_partial_exempt,
        abi.bln_payment_tax_capture,
        abi.bln_tax_use_ic_code,
        abi.cf_apbillitem_text_pesname,
        abi.currency_code,
        abi.customer_name,
        abi.department_name ap_bill_department_name,
        abi.dte_cf_apbillitem_expamtstart,
        abi.dte_entry,
        abi.dte_src_start,
        abi.dte_src_end,
        abi.dts_src_created,
        abi.dts_src_modified,
        abi.employee_name as ap_bill_employee_name,
        abi.entry_description,
        abi.europe_vat_rate,
        abi.exchange_rate,
        abi.form_1099,
        abi.form_1099_box,
        abi.form_1099_type,
        abi.gl_dim_vat_code,
        abi.gl_dimline_tax_detail,
        abi.item_name,
        abi.line_no,
        abi.location_name as ap_bill_location_name,
        abi.offset_gl_account_no,
        abi.offset_gl_account_title,
        abi.parent_entry,
        abi.prentry_offset_account_no,
        abi.previous_offset_account,
        abi.project_name as ap_bill_project_name,
        abi.reclaim,
        abi.record_type,
        abi.record_url,
        abi.retain_age_percentage,
        abi.rpec,
        abi.rpes,
        abi.state,
        abi.vat_amount,
        abi.vat_rate,
        abi.vendor_name,

        -- ap_bill fields
        ab.billto_payto_key,
        ab.created_user_id,
        ab.mega_entity_id,
        ab.module_key,
        ab.payto_tax_id,
        ab.pr_batch_key,
        ab.schop_key,
        ab.ship_to_return_to_key,
        ab.supdoc_id,
        ab.tax_solution_id,
        ab.term_key,
        ab.user_id,
        ab.amt_total_due,
        ab.amt_total_entered,
        ab.amt_trx_total_entered,
        ab.base_currency_code,
        ab.bill_back_template,
        ab.billto_payto_contact_name,
        ab.bln_do_not_process_vat,
        ab.bln_on_hold,
        ab.bln_retain_age_released,
        ab.bln_system_generated,
        ab.contact_tax_group,
        ab.description,
        ab.description_2,
        ab.doc_number,
        ab.dte_rec_payment,
        ab.dte_when_discount,
        ab.dte_when_due,
        ab.dte_when_paid,
        ab.dte_when_posted,
        ab.dts_au_src_created,
        ab.due_in_days,
        ab.financial_entity,
        ab.megaentity_name,
        ab.payment_priority,
        ab.payto_taxgroup_name,
        ab.payto_taxgroup_recordno,
        ab.pr_batch,
        ab.raw_state,
        ab.sender_email,
        ab.shipto_returnto_contact_name,
        ab.tax_entity_number,
        ab.tax_reverse_status,
        ab.term_name,
        ab.term_value,
        ab.trx_entity_due,
        ab.trx_total_due

    from ap_bill_item abi
    left join ap_bill ab on abi.key_ap_bill = ab.key
    left join portal_departments por_dep on abi.department_id = por_dep.intacct_id
    left join portal_locations por_loc on por_loc.intacct_id = abi.location_id
    left join portal_entities por_ent on por_loc.entity_id = por_ent.id
    left join locations_intacct on abi.location_id = locations_intacct.locationid
    left join project on abi.key_project = project.key
    left join employee_int on abi.employee_id = employee_int.intacct_employee_id
    left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link
    where ab.key is not null
)

select * from final
