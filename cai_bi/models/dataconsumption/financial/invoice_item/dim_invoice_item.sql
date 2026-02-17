with 
    invoice_items as (
        select key, hash_key, link, hash_link, key_invoice, hash_key_invoice, key_employee, hash_key_employee, key_parent_entry, hash_key_parent_entry, key_project, 
            hash_key_project, account_key, allocation_key, base_location, created_by_login_id, customer_key, customer_id, department_id, detail_key, employee_id, 
            exch_rate_type_id, gl_vat_code, item_key, item_id, location_id, modified_by_login_id, offset_account_key, projectid, src_created_by, src_modified_by, 
            task_id, task_key, vendor_key, vendor_id, account_title, amt, amt_base_vat, amt_net_of_vat, amt_net_of_vat_base, amt_non_reclaim_vat, amt_non_reclaim_vat_base, 
            amt_total_paid, amt_trx, amt_trx_total_paid, amt_vat, currency_iso_code_base, bln_india_cgst_ar, bln_india_igst_ar, bln_india_rcm_ar, bln_india_sgst_ar, 
            bln_is_retain_age_release, bln_is_summarized, bln_line_item, bln_manual_vat_amount, bln_payment_tax_capture, currency_iso_code, customer_name, department_name, 
            dte_entry, dte_exch_rate, dte_rev_rec_start, dts_src_created, dts_src_modified, employee_name, entry_description, exchange_rate, gl_account_no, gl_account_no_offset, 
            gl_account_title_offset, gl_line_tax_detail, item_name, line_no, location_name, project_name, qty_reclaim_vat_amount, qty_previous_offset_account, qty_reclaim, 
            qty_reclaim_vat_base, qty_retain_age_percentage, qty_retained, qty_reverse_txn_vat, qty_reverse_txn_vat_base, qty_total_selected, qty_trx_discount_applied, 
            qty_trx_total_selected, rate_vat, record_type, record_url, status, subtotal, task_name, vendor_name
        from {{ ref('invoice_item')}} 
        where src_sys_key='int'
    ),
    employee_int as (
        select key, display_name, display_name_lf, link, email_address_work from {{ ref('employee')}}  where src_sys_key = 'int'
    ),
    departments as (
        select record_id, id, display_name, intacct_id
        from {{ source('portal', 'departments') }}
        where _fivetran_deleted = false
    ),
    locations as (
        select record_id, id, display_name, intacct_id
        from {{ source('portal', 'locations') }}
        where _fivetran_deleted = false
    ),
    projects as (
        select key, project_name
        from {{ ref('project')}}
        where src_sys_key='int'
    ),
    invoices as (
        select key, dte_exch_rate, dte_when_posted, entity.record_id as key_entity, entity.display_name as entity_name, entity.currency_id as currency_iso_code_entity
        from {{ ref('invoice')}} inv
        left join {{ source('portal', 'entities') }} entity on inv.key_entity = entity.id
        where inv.src_sys_key='int'
    ),
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_mul
        from {{ ref("ref_fx_rates_timeseries") }}  as cc
        where frm_curr in (select currency from {{ ref("currencies_active") }})
        and to_curr in (select currency from {{ ref("currencies_active") }})
    )
    
select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    inv.key,
    inv.key_invoice,
    emp.link as key_employee,
    inv.key_project,
    invoices.key_entity as key_entity,
    por_loc.record_id as key_location,
    por_dept.record_id as key_department,
    inv.task_key as key_task,
    inv.item_key as key_item,
    inv.vendor_key as key_vendor,
    inv.customer_key as key_customer,
    inv.account_key as key_gl_account,
    inv.allocation_key,
    inv.base_location,
    inv.created_by_login_id,
    inv.customer_id,
    inv.department_id,
    inv.detail_key,
    inv.employee_id,
    inv.exch_rate_type_id,
    inv.gl_vat_code,
    inv.item_id,
    inv.location_id,
    inv.modified_by_login_id,
    inv.offset_account_key,
    inv.projectid,
    inv.src_created_by,
    inv.src_modified_by,
    inv.task_id,
    inv.vendor_id,
    inv.account_title,
    inv.currency_iso_code,
    invoices.currency_iso_code_entity as currency_iso_code_entity,
    inv.currency_iso_code_base,
    cast(ifnull(inv.exchange_rate,1) as number(38,6)) as conversion_rate_entity,
    cast(ifnull(cc_to_usd.fx_rate_mul,1) as number(38,6)) as conversion_rate_usd,
    cast(ifnull(inv.amt_trx,0) as number(38,2)) as amt,
    cast(ifnull(inv.amt,0) as number(38,2)) as amt_entity ,
    cast(conversion_rate_usd * ifnull(inv.amt_trx,0) as number(38,2)) as amt_usd,
    cast(ifnull(inv.amt_net_of_vat,0) as number(38,2)) as amt_net_of_vat,
    cast(ifnull(inv.amt_net_of_vat_base,0) as number(38,2)) as amt_net_of_vat_entity,
    cast(conversion_rate_usd * ifnull(inv.amt_net_of_vat,0) as number(38,2)) as amt_net_of_vat_usd,
    cast(ifnull(inv.amt_non_reclaim_vat,0) as number(38,2)) as amt_non_reclaim_vat,
    cast(ifnull(inv.amt_non_reclaim_vat_base,0) as number(38,2)) as amt_non_reclaim_entity,
    cast(conversion_rate_usd * ifnull(inv.amt_non_reclaim_vat,0) as number(38,2)) as amt_non_reclaim_usd,
    cast(ifnull(inv.amt_trx_total_paid,0) as number(38,2)) as amt_total_paid,
    cast(ifnull(inv.amt_total_paid,0) as number(38,2))as amt_total_paid_entity,
    cast(conversion_rate_usd * ifnull(inv.amt_trx_total_paid,0) as number(38,2)) as amt_total_paid_usd,
    cast(ifnull(inv.amt_vat,0) as number(38,2)) as amt_vat,
    cast(ifnull(inv.amt_base_vat,0) as number(38,2)) as amt_vat_entity,
    cast(conversion_rate_usd * ifnull(inv.amt_vat,0) as number(38,2)) as amt_vat_usd,
    ifnull(inv.bln_india_cgst_ar,false) as bln_india_cgst_ar,
    ifnull(inv.bln_india_igst_ar,false) as bln_india_igst_ar,
    ifnull(inv.bln_india_rcm_ar,false) as bln_india_rcm_ar,
    ifnull(inv.bln_india_sgst_ar,false) as bln_india_sgst_ar,
    ifnull(inv.bln_is_retain_age_release,false) as bln_is_retain_age_release,
    ifnull(inv.bln_is_summarized,false) as bln_is_summarized,
    ifnull(inv.bln_line_item,false) as bln_line_item,
    ifnull(inv.bln_manual_vat_amount,false) as bln_manual_vat_amount,
    ifnull(inv.bln_payment_tax_capture,false) as bln_payment_tax_capture,
    inv.customer_name,
    inv.department_name as int_department_name,
    inv.dte_entry,
    inv.dte_exch_rate,
    inv.dte_rev_rec_start,
    inv.dts_src_created,
    inv.dts_src_modified,
    inv.employee_name as int_employee_name,
    inv.entry_description,
    inv.gl_account_no,
    inv.gl_account_no_offset,
    inv.gl_account_title_offset,
    inv.gl_line_tax_detail,
    inv.item_name,
    inv.line_no,
    inv.location_name as int_location_name,
    inv.project_name as int_project_name,
    ifnull(inv.qty_reclaim_vat_amount,0) as qty_reclaim_vat_amount,
    ifnull(inv.qty_previous_offset_account,0) as qty_previous_offset_account,
    ifnull(inv.qty_reclaim,0) as qty_reclaim,
    ifnull(inv.qty_reclaim_vat_base,0) as qty_reclaim_vat_base,
    ifnull(inv.qty_retain_age_percentage,0) as qty_retain_age_percentage,
    ifnull(inv.qty_retained,0) as qty_retained,
    ifnull(inv.qty_reverse_txn_vat,0) as qty_reverse_txn_vat,
    ifnull(inv.qty_reverse_txn_vat_base,0) as qty_reverse_txn_vat_base,
    ifnull(inv.qty_total_selected,0) as qty_total_selected,
    ifnull(inv.qty_trx_discount_applied,0) as qty_trx_discount_applied,
    ifnull(inv.qty_trx_total_selected,0) as qty_trx_total_selected,
    ifnull(inv.rate_vat,0) as rate_vat,
    inv.record_type,
    inv.record_url,
    inv.status,
    inv.subtotal,
    inv.task_name,
    inv.vendor_name,
    emp.display_name as employee_name,
    emp.display_name_lf as employee_name_lf,
    emp.email_address_work as employee_name_email,
    por_loc.display_name as location_name,
    ifnull(projects.project_name, inv.project_name) as project_name,
    invoices.entity_name as entity_name,
    ifnull(por_dept.display_name, inv.department_name) as department_name,
    inv.task_name as phase_code
from invoice_items inv
left join invoices on inv.key_invoice = invoices.key
left join departments por_dept on inv.department_id = por_dept.intacct_id
left join locations por_loc on inv.location_id = por_loc.intacct_id
left join projects on inv.key_project = projects.key
left join employee_int emp on inv.key_employee = emp.key
left join currency_conversion as cc_to_usd on (
                    cc_to_usd.frm_curr = currency_iso_code
                    and cc_to_usd.to_curr = 'USD'
                    and cc_to_usd.date = (case
                                            when coalesce(inv.dte_exch_rate, invoices.dte_exch_rate, invoices.dte_when_posted) < '2016-01-02' then '2016-01-04' -- earliest date we have is 1/4/2016
                                            else coalesce(inv.dte_exch_rate, invoices.dte_exch_rate, invoices.dte_when_posted)
                                        end)
                )