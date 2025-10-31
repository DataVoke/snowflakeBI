{{ 
    config(
        materialized='table',
        schema='consolidation',
    )
}}

with
    cc_transaction_entry as (select * from {{ source('sage_intacct', 'cc_transaction_entry')}} where _fivetran_deleted = false),
    location as (select * from {{ source('sage_intacct', 'location') }} where _fivetran_deleted = false),

sage_intacct as (
    select
        'int' as src_sys_key,
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        current_timestamp as dts_eff_start,
        '9999-12-31' as dts_eff_end,
        true as bln_current,

        cast(cc_transaction_entry.recordno as string) as key,
        md5(cc_transaction_entry.recordno) as hash_key,

        cast(cc_transaction_entry.recordkey as string) as key_cc_transaction,
        md5(cc_transaction_entry.recordkey) as hash_key_cc_transaction,

        cast(cc_transaction_entry.projectdimkey as string) as key_project,
        md5(cc_transaction_entry.projectdimkey) as hash_key_project,

        cast(coalesce(location.parentkey, location.recordno) as string) as key_entity,
        md5(coalesce(location.parentkey, location.recordno)) as hash_key_entity,

        cast(cc_transaction_entry.employeedimkey as string) as key_employee,
        md5(cc_transaction_entry.employeedimkey) as hash_key_employee,

        cast(location.recordno as string) as key_location,
        md5(location.recordno) as hash_key_location,

        cc_transaction_entry.accountkey as account_key,
        cc_transaction_entry.baselocation as base_location_id,
        cc_transaction_entry.customerdimkey as customer_dim_key,
        cc_transaction_entry.customerid as customer_id,
        cc_transaction_entry.departmentid as department_id,
        cc_transaction_entry.employeeid as employee_id,
        cc_transaction_entry.exch_rate_type_id as exchange_rate_type_id,
        cc_transaction_entry.gldimvat_code as gl_dim_vat_code,
        cc_transaction_entry.itemdimkey as item_dim_key,
        cc_transaction_entry.itemid as item_id,
        cc_transaction_entry.locationid as location_id,
        cc_transaction_entry.projectid as project_id,
        cc_transaction_entry.createdby as src_created_by_id,
        cc_transaction_entry.modifiedby as src_modified_by_id,
        cc_transaction_entry.vendordimkey as vendor_dim_key,
        cc_transaction_entry.vendorid as vendor_id,

        cc_transaction_entry.accounttitle as account_title,
        cc_transaction_entry.amount as amt,
        cc_transaction_entry.totalexpensed as amt_total_expensed,
        cc_transaction_entry.totalpaid as amt_total_paid,
        cc_transaction_entry.totalselected as amt_total_selected,
        cc_transaction_entry.trx_amount as amt_trx,
        cc_transaction_entry.trx_totalpaid as amt_trx_total_paid,
        cc_transaction_entry.trx_totalselected as amt_trx_total_selected,

        cc_transaction_entry.basecurr as base_currency,
        cc_transaction_entry.billable as bln_billable,
        cc_transaction_entry.billed as bln_billed,
        cc_transaction_entry.istax as bln_is_tax,
        cc_transaction_entry.lineitem as bln_line_item,

        cc_transaction_entry.cctxn_payee as cc_txn_payee,
        cc_transaction_entry.currency as currency,
        cc_transaction_entry.customername as customer_name,
        cc_transaction_entry.departmentname as department_name,
        cc_transaction_entry.description as description,

        cc_transaction_entry.exch_rate_date as dte_exchange_rate,
        cc_transaction_entry.whencreated as dts_src_created,
        cc_transaction_entry.whenmodified as dts_src_modified,

        cc_transaction_entry.employeename as employee_name,
        cc_transaction_entry.exchange_rate as exchange_rate,
        cc_transaction_entry.financialentity as financial_entity,
        cc_transaction_entry.itemname as item_name,
        cc_transaction_entry.line_no as line_no,
        cc_transaction_entry.locationname as location_name,
        cc_transaction_entry.projectname as project_name,
        cc_transaction_entry.recordtype as record_type,
        cc_transaction_entry.record_url as record_url,
        cc_transaction_entry.status as status,
        cc_transaction_entry.vendorname as vendor_name

    from
        cc_transaction_entry
    left join
        location
        on cc_transaction_entry.locationid = cast(location.recordno as varchar)
)

select
    src_sys_key,
    cast(dts_created_at as timestamp_tz) as dts_created_at,
    created_by,
    cast(dts_updated_at as timestamp_tz) as dts_updated_at,
    updated_by,
    cast(dts_eff_start as timestamp_tz) as dts_eff_start,
    cast(dts_eff_end as timestamp_tz) as dts_eff_end,
    bln_current,
    key,
    hash_key,
    key_cc_transaction,
    hash_key_cc_transaction,
    key_project,
    hash_key_project,
    key_entity,
    hash_key_entity,
    key_employee,
    hash_key_employee,
    key_location,
    hash_key_location,
    account_key,
    base_location_id,
    customer_dim_key,
    customer_id,
    department_id,
    employee_id,
    exchange_rate_type_id,
    gl_dim_vat_code,
    item_dim_key,
    item_id,
    location_id,
    project_id,
    src_created_by_id,
    src_modified_by_id,
    vendor_dim_key,
    vendor_id,
    account_title,
    cast(amt as number(38, 2)) as amt,
    amt_total_expensed,
    cast(amt_total_paid as number(38, 2)) as amt_total_paid,
    amt_total_selected,
    cast(amt_trx as number(38, 2)) as amt_trx,
    cast(amt_trx_total_paid as number(38, 2)) as amt_trx_total_paid,
    amt_trx_total_selected,
    base_currency,
    bln_billable,
    bln_billed,
    bln_is_tax,
    bln_line_item,
    cc_txn_payee,
    currency,
    customer_name,
    department_name,
    description,
    dte_exchange_rate,
    cast(dts_src_created as timestamp_ntz) as dts_src_created,
    cast(dts_src_modified as timestamp_ntz) as dts_src_modified,
    employee_name,
    cast(exchange_rate as number(38, 6)) as exchange_rate,
    financial_entity,
    item_name,
    line_no,
    location_name,
    project_name,
    record_type,
    record_url,
    status,
    vendor_name
from
    sage_intacct
