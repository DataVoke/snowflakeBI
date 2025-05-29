{{ config(
    materialized='table',
    alias="timesheet_entry"
) }}

with 

    si_timesheetentry as (select * from {{ source('sage_intacct', 'timesheetentry') }} where _fivetran_deleted = false),
    si_timesheet      as (select * from {{ source('sage_intacct', 'timesheet') }} where _fivetran_deleted = false),
    si_filtered       as (
                            select 
                                * 
                            from si_timesheetentry
                            qualify row_number() over (
                                partition by projectid, employeeid, entrydate, taskkey 
                                order by whenmodified desc
                            ) = 1
                        ),
                        
    sf_tasktime as (select * from {{ source('salesforce', 'pse_task_time_c') }} where is_deleted = false),
    sf_timecardheader as (select * from {{ source('salesforce', 'pse_timecard_header_c') }} where is_deleted = false),
    sf_project as (select * from {{ source('salesforce', 'pse_proj_c') }} where is_deleted = false),
    sf_contact as (select * from {{ source('salesforce', 'contact') }} where is_deleted = false),
    sf_projecttask as (select * from {{ source('salesforce', 'pse_project_task_c') }} where is_deleted = false),

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
        si_timesheetentry.taskkey as key_task,
        cast(si_timesheetentry.taskkey as string) as hash_key_task,
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
        null as bill_rate,
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
        tt.id as key,
        md5(tt.id) as hash_key,
        si.recordno as link,
        md5(si.recordno) as hash_link,
        tt.pse_timecard_c as key_timesheet,
        md5(tt.pse_timecard_c) as hash_key_timesheet,
        tt.pse_resource_c as key_employee,
        md5(tt.pse_resource_c) as hash_key_employee,
        c.pse_group_c as key_entity,
        md5(c.pse_group_c) as hash_key_entity,
        p.intacct_record_no_c as key_project,
        md5(p.intacct_record_no_c) as hash_key_project,
        pt.intacct_record_no_c as key_task,
        md5(pt.intacct_record_no_c) as hash_key_task,
        P.INTACCT_PROJECT_ID_C||C.PSE_API_RESOURCE_CORRELATION_ID_C ||PT.INTACCT_ID_C||TT.ENTRYDATE as key_timesheet_entry,
        md5(P.INTACCT_PROJECT_ID_C||C.PSE_API_RESOURCE_CORRELATION_ID_C ||PT.INTACCT_ID_C||TT.ENTRYDATE) as key_timesheet_entry,
        null as BILLU_ACCT_KEY,
        null as CUSTOMER_ID,
        null as DEPARTMENT_ID,
        null as DEPARTMENT_KEY,
        null as EMPLOYEE_EARNING_TYPE_KEY,
        c.PSE_API_RESOURCE_CORRELATION_ID_C as EMPLOYEE_ID_INTACCT,
        null as ITEM_ID,
        null as ITEM_KEY,
        null as LABOR_GL_BATCH_KEY,
        null as LOCATION_ID,
        null as LOCATION_KEY,
        null as NON_BILLNU_ACCT_KEY,
        null as NON_BILLU_ACCT_KEY,
        p.INTACCT_PROJECT_ID_C as PROJECT_ID,
        tt.created_by_id as SRC_CREATED_BY_ID,
        tt.last_modified_by_id as SRC_MODIFIED_BY_ID,
        null as STAT_GL_BATCH_KEY,
        null as STAT_JOURNAL_KEY,
        pt.intacct_id_c as TASK_ID,
        null as AMT_LABOR_GL_ENTRY,
        null as AMT_LABOR_GLENTRY_TRX,
        null as AMT_STAT_GL_ENTRY,
        null as BILL_RATE,
        null as BLN_BILLABLE,
        null as BLN_BILLED,
        null as CUSTOMER_NAME,
        c.department as DEPARTMENT_NAME,
        tt.entrydate as DTE_ENTRY,
        null as DTE_GL_POST,
        tt.created_date as DTE_SRC_CREATED,
        tt.pse_end_date_c as DTE_SRC_END,
        tt.last_modified_date as DTE_SRC_MODIFIED,
        tt.entrydate as DTE_SRC_START,
        c.name as EMPLOYEE_NAME,
        null as ITEM_NAME,
        null as LABOR_GL_ENTRY_COST_RATE,
        null as LABOR_GL_ENTRY_LINE_NO,
        null as LABOR_GL_ENTRY_OFFSET_LINE_NO,
        null as LINE_NO,
        null as LOCATION_NAME,
        tt.notes,
        p.name as project_name,
        tt.hours as qty,
        null as QTY_APPROVED,
        null as QTY_APPROVED_BILLABLE,
        null as QTY_APPROVED_NON_BILLABLE,
        null as QTY_APPROVED_NON_UTILIZED,
        null as QTY_APPROVED_UTILIZED,
        null as QTY_BILLABLE,
        null as QTY_NON_BILLABLE,
        null as QTY_NON_UTILIZED,
        null as QTY_UTILIZED,
        null as RECORD_URL,
        null as STAT_GL_ENTRY_LINE_NO,
        null as STATE,
        tt.name
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
            tt.name
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
            tt.name
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
            tt.name
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
            tt.name
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
            tt.name
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
            tt.name
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
            tt.name
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
)

select * from sage_intacct
union all
select * from salesforce
