with

    si_timesheetentry    as (select * from dev_bi_raw.sage_intacct_ldg.timesheetentry where _fivetran_deleted = false),
    si_timesheet         as (select * from dev_bi_raw.sage_intacct_ldg.timesheet where _fivetran_deleted = false),
    si_project_resources as (select * from dev_bi_raw.sage_intacct_ldg.project_resources where _fivetran_deleted = false),
    si_project           as (select * from dev_bi_raw.sage_intacct_ldg.project where _fivetran_deleted = false),
    si_filtered          as (
                            select
                                *
                            from si_timesheetentry
                            qualify row_number() over (
                                partition by projectid, employeeid, entrydate, taskid
                                order by whenmodified desc
                            ) = 1
                        ),

    sf_tasktime         as (select * from dev_bi_raw.salesforce.pse_task_time_c where is_deleted = false),
    sf_timecardheader   as (select * from dev_bi_raw.salesforce.pse_timecard_header_c where is_deleted = false),
    sf_project          as (select * from dev_bi_raw.salesforce.pse_proj_c where is_deleted = false),
    sf_contact          as (select * from dev_bi_raw.salesforce.contact where is_deleted = false),
    sf_projecttask      as (select * from dev_bi_raw.salesforce.pse_project_task_c where is_deleted = false),
    billrate_currency   as (
        with base as (
            select
                pr.recordno,
                pr.employeekey,
                pr.employeeid,
                pr.projectkey,
                pr.projectid,
                pr.itemkey,
                pr.itemid,
                pr.employeecontactname,
                pr.billingrate,
                p.currency,
                ifnull(pr.startdate, '1900-01-01') as effectivedate,
                concat(ifnull(pr.employeeid,''), ifnull(pr.projectid,''), ifnull(pr.itemid,'')) as currentid
            from si_project_resources pr
            left join si_project p on (pr.projectid = p.projectid)
        ),
        with_date_ranges as (
            select *,
                effectivedate as date_from,
                ifnull(lead(ifnull(effectivedate,'9999-12-31')) over (partition by currentid order by effectivedate asc ) - interval '1 day','9999-12-31')  as date_to
            from base
        ),
        matched_items as(
            select
            t.recordno as timerecordno,
            t.projectid as timeprojectid,
            t.employeeid,
            t.entrydate,
            t.taskid,
            t.taskkey,
            t.itemid,
            t.itemkey,
            t.employeedimkey,
            r.recordno,
            r.employeekey,
            r.employeeid,
            r.projectkey,
            r.projectid,
            r.itemkey,
            r.itemid,
            r.employeecontactname,
            r.billingrate,
            r.currency,
            r.effectivedate,
            r.date_from,
            r.date_to
            from (
                select   *
                from si_timesheetentry
                    qualify row_number() over ( partition by projectid, employeeid, entrydate, taskid order by whenmodified desc ) =1
            ) as t
            inner join with_date_ranges as r on (r.projectkey = t.projectkey and r.employeekey = t.employeedimkey and r.itemkey = t.itemkey and (t.entrydate between date_from and date_to))
        ),
        unmatched_items as (
            select
            t.recordno as timerecordno,
            t.projectid as timeprojectid,
            t.employeeid,
            t.entrydate,
            t.taskid,
            t.taskkey,
            t.itemid,
            t.itemkey,
            t.employeedimkey,
            r.recordno,
            r.employeekey,
            r.employeeid,
            r.projectkey,
            r.projectid ,
            r.itemkey,
            r.itemid,
            r.employeecontactname,
            r.billingrate,
            r.currency,
            r.effectivedate,
            r.date_from,
            r.date_to
            from (
                select   *
                from si_timesheetentry
                    qualify row_number() over ( partition by projectid, employeeid, entrydate, taskid order by whenmodified desc ) =1
            ) as t
            left join with_date_ranges as r on (r.projectkey = t.projectkey and  r.employeekey = t.employeedimkey and r.itemkey is null and (t.entrydate between date_from and date_to))
            where timerecordno not in (select timerecordno from matched_items)
        ),
        billrate_currency_final as (
            select *
            from matched_items
            union(
                select * from unmatched_items
            )
        )

        select * from billrate_currency_final
        ),

sage_intacct as (
    select
        'int' as src_sys_key,
        current_timestamp as dts_created_at,
        'timesheet_entry' as created_by,
        current_timestamp as dts_updated_at,
        'timesheet_entry' as updated_by,
        current_timestamp as dts_eff_start,
        '9999-12-31' as dts_eff_end,
        true as bln_current,
        si_timesheetentry.recordno as key,
        md5(si_timesheetentry.recordno) as hash_key,
        si_timesheetentry.recordno as link,
        md5(si_timesheetentry.recordno) as hash_link,
        cast(si_timesheetentry.timesheetkey as string) as key_timesheet,
        md5(si_timesheetentry.timesheetkey) as hash_key_timesheet,
        cast(si_timesheetentry.employeedimkey as string) as key_employee,
        md5(si_timesheetentry.employeedimkey) as hash_key_employee,
        cast(si_timesheet.megaentitykey as string) as key_entity,
        md5(si_timesheet.megaentitykey) as hash_key_entity,
        cast(si_timesheetentry.projectkey as string) as key_project,
        md5(si_timesheetentry.projectkey) as hash_key_project,
        cast(si_timesheetentry.taskkey as string) as key_task,
        md5(si_timesheetentry.taskkey) as hash_key_task,
        concat(si_timesheetentry.projectid, si_timesheetentry.employeeid, si_timesheetentry.taskid, si_timesheetentry.entrydate) as key_timesheet_entry,
        md5(concat(si_timesheetentry.projectid, si_timesheetentry.employeeid, si_timesheetentry.taskid, si_timesheetentry.entrydate)) as hash_key_timesheet_entry,
        si_timesheetentry.billuacctkey as billu_acct_key,
        si_timesheetentry.customerid as customer_id,
        si_timesheetentry.departmentid as department_id,
        si_timesheetentry.departmentkey as department_key,
        si_timesheetentry.employee_earningtypekey as employee_earning_type_key,
        si_timesheetentry.employeeid as employee_id_intacct,
        si_timesheetentry.itemid as item_id,
        cast(si_timesheetentry.itemkey as string) as item_key,
        cast(si_timesheetentry.laborglbatchkey as string) as labor_gl_batch_key,
        si_timesheetentry.locationid as location_id,
        cast(si_timesheetentry.locationkey as string) as location_key,
        cast(si_timesheetentry.nonbillnuacctkey as string) as non_billnu_acct_key,
        cast(si_timesheetentry.nonbilluacctkey as string) as non_billu_acct_key,
        si_timesheetentry.projectid as project_id,
        cast(si_timesheetentry.createdby as string) as src_created_by_id,
        cast(si_timesheetentry.modifiedby as string) as src_modified_by_id,
        cast(si_timesheetentry.statglbatchkey as string) as stat_gl_batch_key,
        cast(si_timesheetentry.statjournalkey as string) as stat_journal_key,
        si_timesheetentry.taskid as task_id,
        si_timesheetentry.laborglentryamount as amt_labor_gl_entry,
        si_timesheetentry.laborglentrytrxamount as amt_labor_glentry_trx,
        si_timesheetentry.statglentryamount as amt_stat_gl_entry,
        billrate_currency.billingrate as bill_rate,
        billrate_currency.currency as currency_code,
        si_timesheetentry.billable as bln_billable,
        si_timesheetentry.billed as bln_billed,
        si_timesheetentry.customername as customer_name,
        si_timesheetentry.departmentname as department_name,
        si_timesheetentry.entrydate as dte_entry,
        si_timesheetentry.ts_glpostdate as dte_gl_post,
        si_timesheetentry.whencreated as dte_src_created,
        si_timesheetentry.ts_enddate as dte_src_end,
        si_timesheetentry.whenmodified as dte_src_modified,
        si_timesheetentry.ts_begindate as dte_src_start,
        si_timesheetentry.employeename as employee_name,
        si_timesheetentry.itemname as item_name,
        si_timesheetentry.laborglentrycostrate as labor_gl_entry_cost_rate,
        si_timesheetentry.laborglentrylineno as labor_gl_entry_line_no,
        si_timesheetentry.laborglentryoffsetlineno as labor_gl_entry_offset_line_no,
        si_timesheetentry.lineno as line_no,
        si_timesheetentry.locationname as location_name,
        si_timesheetentry.notes as notes,
        si_timesheetentry.projectname as project_name,
        si_timesheetentry.qty as qty,
        si_timesheetentry.approved_qty as qty_approved,
        si_timesheetentry.approved_billable_qty as qty_approved_billable,
        si_timesheetentry.approved_non_billable_qty as qty_approved_non_billable,
        si_timesheetentry.approved_non_utilized_qty as qty_approved_non_utilized,
        si_timesheetentry.approved_utilized_qty as qty_approved_utilized,
        si_timesheetentry.billable_qty as qty_billable,
        si_timesheetentry.non_billable_qty as qty_non_billable,
        si_timesheetentry.non_utilized_qty as qty_non_utilized,
        si_timesheetentry.utilized_qty as qty_utilized,
        si_timesheetentry.record_url as record_url,
        si_timesheetentry.statglentrylineno as stat_gl_entry_line_no,
        si_timesheetentry.state as state,
        si_timesheetentry.taskname as task_name
    from si_timesheetentry
    left join si_timesheet on si_timesheet.recordno = si_timesheetentry.timesheetkey
    left join billrate_currency on billrate_currency.timerecordno = si_timesheetentry.recordno
)

    select * from sage_intacct;