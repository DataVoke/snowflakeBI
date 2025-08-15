{{ config(
    materialized='table',
    alias="expense_item"
) }}

with
int_expense_item as (
    select * 
    from {{ ref('expenseitem')}} 
    where src_sys_key = 'int' and bln_line_item = true
),
sfc_expense_item as (
    select * 
    from {{ ref('expenseitem')}} 
    where src_sys_key = 'sfc'
),
int_expense as (
    select * 
    from {{ ref('expense') }} 
    where src_sys_key = 'int'
),
sfc_expense as (
    select * 
    from {{ ref('expense') }} 
    where src_sys_key = 'sfc'
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
    from {{ source('sage_intacct', 'location')}}
),
project as (
    select * 
    from {{ ref('project')}} 
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
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        int_ei.key,
        int_ei.key_expense,

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
        project.currency_iso_code,

        -- direct fields
        int_ei.account_key,
        int_ei.account_label_key,
        int_ei.customer_key,
        int_ei.customer_id,
        int_ei.department_id,
        ifnull(nullif(locations_intacct.parentid, ''), int_ei.location_id) as entity_id,
        int_ei.detail_key,
        int_ei.employee_id,
        int_ei.expense_item_id,
        int_ei.exp_pmt_type_key,
        int_ei.item_key,
        int_ei.item_id,
        int_ei.location_id,
        sfc_ei.milestone_id,
        int_ei.project_id as int_project_id,
        int_ei.src_created_by,
        int_ei.src_modified_by,
        int_ei.vendor_key,
        int_ei.vendor_id,
        sfc_e.approver_id exp_approver_id,
        sfc_e.assignment_id exp_sfc_assignment_id,
        sfc_e.contact_id exp_contact_id,
        int_e.record_id exp_record_id,
        int_e.tax_solution_id exp_tax_solution_id,
        int_ei.account_label,
        int_ei.account_no,
        int_ei.amt,
        int_ei.amt_base_vat,
        int_ei.amt_gl_posting,
        int_ei.amt_manual_vat,
        int_ei.amt_net_of_vat,
        int_ei.amt_net_of_vat_base,
        int_ei.amt_non_reclaim_vat_base_amount,
        int_ei.amt_nr,
        int_ei.amt_trx_nr,
        int_ei.amt_reclaim_vat,
        int_ei.amt_reclaim_vat_base,
        int_ei.amt_reverse_txn_vat,
        int_ei.amt_reverse_txn_vat_base,
        int_ei.amt_vat,
        int_ei.base_location,
        int_ei.bln_billable,
        int_ei.bln_billed,
        --sfc_ei.currency_iso_code,
        int_ei.customer_name,
        int_ei.description,
        int_ei.description_2,
        sfc_ei.distance,
        int_ei.dte_entry,
        int_ei.employee_name as int_employee_name,
        int_ei.expense_detail_reporting_category,
        sfc_ei.expense_type_detail,
        int_ei.form_1099,
        int_ei.gl_account_no,
        int_ei.gl_account_title,
        int_ei.gl_dim_line_tax_detail,
        int_ei.gl_dim_vat_code,
        int_ei.item_name,
        int_ei.bln_line_item,
        int_ei.line_no,
        int_ei.location_name int_location_name,
        int_ei.non_reclaim_vat_base_amount,
        int_ei.bln_non_reimbursable,
        int_ei.amt_org,
        int_ei.org_currency,
        int_ei.org_exchrate,
        int_ei.dte_org_exchrate,
        int_ei.org_exchratetype,
        int_ei.projectname,
        int_ei.psa_url,
        sfc_ei.assignment_id sfc_assignment_id,
        sfc_ei.audit_notes sfc_audit_notes,
        sfc_ei.bln_lost_receipt,
        sfc_ei.notes,
        sfc_ei.amt_reimbursement_in_project_currency,
        sfc_ei.reimbursement_currency,
        sfc_ei.type,
        int_ei.qty,
        int_ei.reclaim,
        int_ei.record_type,
        int_ei.record_url,
        int_ei.state,
        int_ei.tax_use_ic_code,
        int_ei.amt_total_paid,
        int_ei.amt_total_selected,
        int_ei.amt_trx,
        int_ei.amt_trx_total_paid,
        int_ei.amt_trx_total_selected,
        int_ei.unit_rate,
        int_ei.user_exch_rate,
        int_ei.vat_rate,
        int_ei.vendor_name,
        sfc_e.amt_total_billable exp_amt_total_billable,
        sfc_e.amt_total_non_reimbursement exp_amt_total_non_reimbursement,
        sfc_e.amt_total_reimbursement exp_amt_total_reimbursement,
        sfc_e.audit_notes exp_audit_notes,
        int_e.base_currency exp_base_currency,
        sfc_e.bln_approved exp_bln_approved,
        int_e.bln_pr_batch_nogl exp_bln_pr_batch_nogl,
        int_e.bln_inclusive_tax exp_bln_inclusive_tax,
        sfc_e.bln_submitted exp_bln_submitted,
        int_e.dts_audit_when_created exp_dts_audit_when_created,
        sfc_e.dte_first_expense exp_dte_first_expense,
        sfc_e.dte_last_expense exp_dte_last_expense,
        int_e.currency exp_currency,
        int_e.first_name exp_first_name,
        int_e.last_name exp_last_name,
        int_e.mega_entity_name exp_mega_entity_name,
        int_e.memo exp_memo,
        int_e.pr_batch exp_pr_batch,
        int_e.pr_batch_key exp_pr_batch_key,
        int_e.pr_batch_open exp_pr_batch_open,
        int_e.raw_state exp_raw_state,
        sfc_e.sync_status exp_sync_status,
        int_e.total_due exp_total_due,
        int_e.total_entered exp_total_entered,
        int_e.total_nr_entered exp_total_nr_entered,
        int_e.total_nr_trx_entered exp_total_nr_trx_entered,
        int_e.total_trx_due exp_total_trx_due,
        int_e.total_trx_entered exp_total_trx_entered,
        int_e.total_trx_paid exp_total_trx_paid,
        int_e.dte_when_paid exp_dte_when_paid,
        int_e.dte_when_posted exp_dte_when_posted,
        int_e.dte_when_submitted exp_dte_when_submitted
    from int_expense_item int_ei
    left join sfc_expense_item sfc_ei on int_ei.hash_link = sfc_ei.hash_link
    left join int_expense int_e on int_ei.hash_key_expense = int_e.hash_key
    left join sfc_expense sfc_e on int_e.hash_link = sfc_e.hash_link
    left join portal_departments por_dep on int_ei.department_id = por_dep.intacct_id
    left join portal_locations por_loc on por_loc.intacct_id = int_ei.location_id
    --left join portal_entities por_ent on por_loc.entity_id = por_ent.id
    left join locations_intacct on int_ei.location_id = locations_intacct.locationid
    left join portal_entities por_ent on coalesce(locations_intacct.parentkey,int_ei.base_location) = por_ent.id
    left join project on int_ei.hash_key_project = project.hash_key
    left join employee_int on int_ei.employee_id = employee_int.intacct_employee_id
    left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link
    where int_e.key is not null
)

select * from final
