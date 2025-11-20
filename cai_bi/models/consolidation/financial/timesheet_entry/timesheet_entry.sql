{{ config(
    materialized='table',
    schema="consolidation",
    alias="timesheet_entry"
) }}

with 

    si_timesheetentry    as (select * from {{ source('sage_intacct', 'timesheetentry') }} where _fivetran_deleted = false),
    si_timesheet         as (select * from {{ source('sage_intacct', 'timesheet') }} where _fivetran_deleted = false),
    si_project_resources as (select * from {{ source('sage_intacct', 'project_resources') }} where _fivetran_deleted = false),
    si_project           as (select * from {{ source('sage_intacct', 'project') }} where _fivetran_deleted = false),
    si_timesheetentry_filtered          as (
                            select 
                                * 
                            from si_timesheetentry
                            qualify row_number() over (
                                partition by projectid, employeeid, entrydate, taskkey 
                                order by whenmodified desc
                            ) = 1
                        ),
                        
    sf_tasktime         as (select * from {{ source('salesforce', 'pse_task_time_c') }} where is_deleted = false),
    sf_timecardheader   as (select * from {{ source('salesforce', 'pse_timecard_header_c') }} where is_deleted = false),
    sf_project          as (select * from {{ source('salesforce', 'pse_proj_c') }} where is_deleted = false),
    sf_contact          as (select * from {{ source('salesforce', 'contact') }} where is_deleted = false),
    sf_projecttask      as (select * from {{ source('salesforce', 'pse_project_task_c') }} where is_deleted = false),
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
            r.billingrate,  
            r.currency, 
            r.effectivedate,
            r.date_from,
            r.date_to
            from si_timesheetentry_filtered as t 
            inner join with_date_ranges as r on (r.projectkey = t.projectkey and r.employeekey = t.employeedimkey and r.itemkey = t.itemkey and (t.entrydate between date_from and date_to))    
        ),
        unmatched_items as (
            select 
            t.recordno as timerecordno,
            t.projectid as timeprojectid, 
            t.employeeid,  
            t.entrydate, 
            t.taskid,  
            r.billingrate,  
            r.currency, 
            r.effectivedate,
            r.date_from,
            r.date_to
            from si_timesheetentry_filtered as t  
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
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
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
    from si_timesheetentry_filtered as si_timesheetentry
    left join si_timesheet on si_timesheet.recordno = si_timesheetentry.timesheetkey
    left join billrate_currency on billrate_currency.timerecordno = si_timesheetentry.recordno
),

salesforce as (
    select
        'sfc' as src_sys_key,
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        current_timestamp as dts_eff_start,
        '9999-12-31' as dts_eff_end,
        true as bln_current,
        tt.id || tt.entrydate as key,
        md5(tt.id || tt.entrydate) as hash_key,
        si.recordno as link,
        md5(si.recordno) as hash_link,
        tt.pse_timecard_c as key_timesheet,
        md5(tt.pse_timecard_c) as hash_key_timesheet,
        tt.pse_resource_c as key_employee,
        md5(tt.pse_resource_c) as hash_key_employee,
        c.pse_group_c as key_entity,
        md5(c.pse_group_c) as hash_key_entity,
        p.id as key_project,
        md5(p.id) as hash_key_project,
        pt.intacct_record_no_c as key_task,
        md5(pt.intacct_record_no_c) as hash_key_task,
        p.intacct_project_id_c||c.pse_api_resource_correlation_id_c ||pt.intacct_id_c||tt.entrydate as key_timesheet_entry,
        md5(p.intacct_project_id_c||c.pse_api_resource_correlation_id_c ||pt.intacct_id_c||tt.entrydate) as key_timesheet_entry,
        null as billu_acct_key,
        null as customer_id,
        null as department_id,
        null as department_key,
        null as employee_earning_type_key,
        c.pse_api_resource_correlation_id_c as employee_id_intacct,
        null as item_id,
        null as item_key,
        null as labor_gl_batch_key,
        null as location_id,
        null as location_key,
        null as non_billnu_acct_key,
        null as non_billu_acct_key,
        p.intacct_project_id_c as project_id,
        tt.created_by_id as src_created_by_id,
        tt.last_modified_by_id as src_modified_by_id,
        null as stat_gl_batch_key,
        null as stat_journal_key,
        pt.intacct_id_c as task_id,
        null as amt_labor_gl_entry,
        null as amt_labor_glentry_trx,
        null as amt_stat_gl_entry,
        null as bill_rate,
        null as currency_code,
        null as bln_billable,
        null as bln_billed,
        null as customer_name,
        c.department as department_name,
        tt.entrydate as dte_entry,
        null as dte_gl_post,
        tt.created_date as dte_src_created,
        tt.pse_end_date_c as dte_src_end,
        tt.last_modified_date as dte_src_modified,
        tt.entrydate as dte_src_start,
        c.name as employee_name,
        null as item_name,
        null as labor_gl_entry_cost_rate,
        null as labor_gl_entry_line_no,
        null as labor_gl_entry_offset_line_no,
        null as line_no,
        null as location_name,
        tt.notes,
        p.name as project_name,
        tt.hours as qty,
        null as qty_approved,
        null as qty_approved_billable,
        null as qty_approved_non_billable,
        null as qty_approved_non_utilized,
        null as qty_approved_utilized,
        null as qty_billable,
        null as qty_non_billable,
        null as qty_non_utilized,
        null as qty_utilized,
        null as record_url,
        null as stat_gl_entry_line_no,
        th.pse_status_c as state,
        tt.phase_code_name_c as task_name
    from (
        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_sunday_hours_c, 0) as hours,
            th.pse_sunday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_sunday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 1 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_monday_hours_c, 0) as hours,
            th.pse_monday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_monday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 2 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_tuesday_hours_c, 0) as hours,
            th.pse_tuesday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_tuesday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 3 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_wednesday_hours_c, 0) as hours,
            th.pse_wednesday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_wednesday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 4 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_thursday_hours_c, 0) as hours,
            th.pse_thursday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_thursday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 5 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_friday_hours_c, 0) as hours,
            th.pse_friday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_friday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false

        union

        select
            tt.id,
            tt.pse_timecard_c,
            tt.pse_project_task_c,
            tt.pse_start_date_c + 6 as entrydate,
            th.pse_resource_c,
            nullif(tt.pse_saturday_hours_c, 0) as hours,
            th.pse_saturday_notes_c as notes,
            tt.created_by_id,
            tt.last_modified_by_id,
            tt.created_date,
            th.pse_end_date_c,
            th.last_modified_date,
            tt.name,
            tt.phase_code_name_c
        from sf_tasktime tt
        left join sf_timecardheader th
            on tt.pse_timecard_c = th.id
        where nullif(tt.pse_saturday_hours_c, 0) <> 0
            and tt.is_deleted = false
            and th.is_deleted = false
    ) tt
    left join sf_timecardheader th
        on tt.pse_timecard_c = th.id
    left join sf_project p
        on th.pse_project_c = p.id
    left join sf_contact c
        on th.pse_resource_c = c.id
    left join sf_projecttask pt
        on tt.pse_project_task_c = pt.id
    left join (
        select *
        from si_timesheetentry
        qualify row_number() over (
            partition by projectid, employeeid, entrydate, taskid
            order by whenmodified desc
        ) = 1
    ) si
        on p.intacct_project_id_c = si.projectid
    and c.pse_api_resource_correlation_id_c = si.employeeid
    and tt.entrydate = si.entrydate
    and pt.intacct_id_c = si.taskid
),
final as (
    select * from sage_intacct
    union all
    select * from salesforce
)
SELECT
    src_sys_key,
    cast(dts_created_at as timestamp_tz) as dts_created_at,
    created_by,
    cast(dts_updated_at as timestamp_tz) as dts_updated_at,
    updated_by,
    cast(dts_eff_start as timestamp_tz) as dts_eff_start,
    cast(bln_current as boolean) as bln_current,
    cast(dts_eff_end as timestamp_tz) as dts_eff_end,
    key,
    hash_key,
    link,
    hash_link,
    key_timesheet,
    hash_key_timesheet,
    key_employee,
    hash_key_employee,
    key_entity,
    hash_key_entity,
    key_project,
    hash_key_project,
    key_task,
    hash_key_task,
    key_timesheet_entry,
    hash_key_timesheet_entry,
    billu_acct_key,
    customer_id,
    department_id,
    department_key,
    employee_earning_type_key,
    employee_id_intacct,
    item_id,
    cast(item_key as number(38, 0)) as item_key,
    cast(labor_gl_batch_key as number(38, 0)) as labor_gl_batch_key,
    location_id,
    cast(location_key as number(38, 0)) as location_key,
    cast(non_billnu_acct_key as number(38, 0)) as non_billnu_acct_key,
    cast(non_billu_acct_key as number(38, 0)) as non_billu_acct_key,
    project_id,
    src_created_by_id,
    src_modified_by_id,
    cast(stat_gl_batch_key as number(38, 0)) as stat_gl_batch_key,
    cast(stat_journal_key as number(38, 0)) as stat_journal_key,
    task_id,
    cast(amt_labor_gl_entry as number(38, 17)) as amt_labor_gl_entry,
    cast(amt_labor_glentry_trx as number(38, 17)) as amt_labor_glentry_trx,
    cast(amt_stat_gl_entry as number(38, 17)) as amt_stat_gl_entry,
    cast(bill_rate as number(38, 17)) as bill_rate,
    currency_code,
    bln_billable,
    bln_billed,
    customer_name,
    department_name,
    dte_entry,
    dte_gl_post,
    dte_src_created,
    dte_src_end,
    dte_src_modified,
    dte_src_start,
    employee_name,
    item_name,
    cast(labor_gl_entry_cost_rate as number(38, 17)) as labor_gl_entry_cost_rate,
    labor_gl_entry_line_no,
    labor_gl_entry_offset_line_no,
    line_no,
    location_name,
    notes,
    project_name,
    cast(qty as number(38, 17)) as qty,
    cast(qty_approved as number(38, 17)) as qty_approved,
    cast(qty_approved_billable as number(38, 17)) as qty_approved_billable,
    cast(qty_approved_non_billable as number(38, 17)) as qty_approved_non_billable,
    cast(qty_approved_non_utilized as number(38, 17)) as qty_approved_non_utilized,
    cast(qty_approved_utilized as number(38, 17)) as qty_approved_utilized,
    cast(qty_billable as number(38, 17)) as qty_billable,
    cast(qty_non_billable as number(38, 17)) as qty_non_billable,
    cast(qty_non_utilized as number(38, 17)) as qty_non_utilized,
    cast(qty_utilized as number(38, 17)) as qty_utilized,
    record_url,
    stat_gl_entry_line_no,
    state,
    task_name
FROM final
