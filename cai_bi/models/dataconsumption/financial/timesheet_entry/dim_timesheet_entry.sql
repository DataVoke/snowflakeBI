{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="timesheet_entry"
    )
}}

with

    int as (select * from {{ ref('timesheet_entry') }}),
    sfc as (
        select 
            hash_link, 
            sum(qty) as qty, 
            listagg(notes, ', ') within group(order by notes) as notes
        from int
        where src_sys_key = 'sfc'
        group by hash_link 
    ),
    departments as (select * from {{ source('portal', 'departments') }} where _fivetran_deleted = false),
    locations as (select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false),
    locations_intacct as (select * from {{ source('sage_intacct', 'location') }} where _fivetran_deleted = false),
    entities as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    practice_areas as (select * from {{ source('portal', 'practice_areas') }} where _fivetran_deleted = false),
    project as (select * from {{ ref('project') }}),
    employee_int as ( select * from {{ ref('employee') }} where src_sys_key = 'int' ),
    employee_ukg as ( select * from {{ ref('employee') }} where src_sys_key = 'ukg'),
    vw_employee_pay as (select * from {{ ref('vw_employee_pay_history') }}),
    currencies_active as (
        select * from {{ ref("currencies_active") }}
    ),
    fx_rates_timeseries as (
        select * from {{ ref("ref_fx_rates_timeseries") }}
    ),
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_mul
        from fx_rates_timeseries as cc
        where frm_curr in (select currency from currencies_active)
        and to_curr in (select currency from currencies_active)
    )
    select 
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        -- keys
        int.key,
        int.key_timesheet,
        departments.record_id as key_department,
        locations.record_id as key_location,
        entities.record_id as key_entity,
        practice_areas.record_id as key_practice_area,
        project.key as key_project,
        employee_ukg.key as key_employee,
        -- --names
        departments.display_name as department_name,
        locations.display_name as location_name,
        entities.display_name as entity_name,
        practice_areas.display_name as practice_area_name,
        project.project_id as project_id,
        project.project_name as project_name,
        project.currency_iso_code,
        ifnull(employee_ukg.display_name, initcap(int.employee_name)) as employee_name,
        ifnull(employee_ukg.display_name_lf, initcap(int.employee_name)) as employee_name_lf,
        -- fields
        int.billu_acct_key,
        int.customer_id,
        int.department_id,
        int.department_key,
        int.employee_earning_type_key,
        int.employee_id_intacct,
        ifnull(nullif(locations_intacct.parentid,''),int.location_id) as entity_id,
        int.item_id,
        int.labor_gl_batch_key,
        int.location_id,
        int.location_key,
        int.non_billnu_acct_key,
        int.non_billu_acct_key,
        int.stat_gl_batch_key,
        int.stat_journal_key,
        int.task_id,
        cast(ifnull(int.amt_labor_gl_entry,0) as number(38,2)) as amt_labor_gl_entry,
        cast(ifnull(int.amt_labor_glentry_trx,0) as number(38,2)) as amt_labor_glentry_trx,
        cast(ifnull(int.amt_stat_gl_entry,0) as number(38,2)) as amt_stat_gl_entry,
        cast(ifnull(int.bill_rate,0) as number(38,2)) as bill_rate,
        int.bln_billable,
        int.bln_billed,
        int.customer_name,
        cast(int.dte_entry as date) as dte_entry,
        cast(int.dte_gl_post as date) as dte_gl_post,
        int.dte_src_created,
        cast(int.dte_src_end as date) as dte_src_end,
        int.dte_src_modified,
        cast(int.dte_src_start as date) as dte_src_start,
        int.item_name,
        cast(ifnull(int.labor_gl_entry_cost_rate,0) as number(38,2)) as labor_gl_entry_cost_rate,
        int.labor_gl_entry_line_no,
        int.labor_gl_entry_offset_line_no,
        int.line_no,
        ifnull(nullif(sfc.notes,''),int.notes) as notes,
        cast(int.qty as number(38,2)) as qty,
        cast(sfc.qty as number(38,2)) as qty_salesforce,
        cast(int.qty_approved as number(38,2)) as qty_approved,
        cast(int.qty_approved_billable as number(38,2)) as qty_approved_billable,
        cast(int.qty_approved_non_billable as number(38,2)) as qty_approved_non_billable,
        cast(int.qty_approved_non_utilized as number(38,2)) as qty_approved_non_utilized,
        cast(int.qty_approved_utilized as number(38,2)) as qty_approved_utilized,
        cast(int.qty_billable as number(38,2)) as qty_billable,
        cast(int.qty_non_billable as number(38,2)) as qty_non_billable,
        cast(int.qty_non_utilized as number(38,2)) as qty_non_utilized,
        cast(int.qty_utilized as number(38,2)) as qty_utilized,
        int.record_url,
        int.stat_gl_entry_line_no,
        int.state,
        sfc.state as sfc_status,
        int.key_task as task_key,
        int.key_timesheet_entry as timesheet_entry_ref,
        int.task_name,
        upper(ifnull(pay.currency_code_original, employee_int.currency_code))  as currency_code_employee_original,
        cast(ifnull(pay.hourly_pay_rate_original,  ifnull(labor_gl_entry_cost_rate,0)) as number(38,2))  as employee_pay_hourly_rate_original,
        cast(ifnull(pay.hourly_pay_rate_cola_original, ifnull(labor_gl_entry_cost_rate,0)) as number(38,2)) as employee_pay_hourly_rate_cola_original,
        upper(ifnull(pay.currency_code_entity, entities.currency_id)) as currency_code_employee_entity,
        cast(ifnull(pay.hourly_pay_rate_entity, ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_entity.fx_rate_mul,1)) as number(38,2)) as employee_pay_hourly_rate_entity,
        cast(ifnull(pay.hourly_pay_rate_cola_entity, ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_entity.fx_rate_mul,1)) as number(38,2)) as employee_pay_hourly_rate_cola_entity,
        cast(ifnull(pay.hourly_pay_rate_usd, ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_usd.fx_rate_mul,1)) as number(38,2)) as employee_pay_hourly_rate_usd,
        cast(ifnull(pay.hourly_pay_rate_cola_usd, ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_usd.fx_rate_mul,1)) as number(38,2)) as employee_pay_hourly_rate_cola_usd,
        ifnull(
            cast(pay.hourly_pay_rate_original * ifnull(cc_pay_to_project.fx_rate_mul,1) as number(38,2)),
            cast(ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_project.fx_rate_mul,1) as number(38,2))
        ) as employee_pay_hourly_rate_project,
        ifnull(
            cast(pay.hourly_pay_rate_cola_original * ifnull(cc_pay_to_project.fx_rate_mul,1) as number(38,2)),
            cast(ifnull(labor_gl_entry_cost_rate,0) * ifnull(cc_ic_to_project.fx_rate_mul,1) as number(38,2))
        ) as employee_pay_hourly_rate_cola_project,
        upper(project.currency_iso_code) as currency_code_project,
        cast(ifnull(int.bill_rate,0) * ifnull(cc_bill_to_original.fx_rate_mul,1) as number(38,2)) as bill_rate_employee_original,
        cast(ifnull(int.bill_rate,0) * ifnull(cc_bill_to_entity.fx_rate_mul,1) as number(38,2)) as bill_rate_employee_entity,
        cast(ifnull(int.bill_rate,0) * ifnull(cc_bill_to_usd.fx_rate_mul,1) as number(38,2)) as bill_rate_employee_usd
    from int
    left join (
        select hash_link, state, sum(qty) as qty, listagg(notes, ', ') within group(order by notes) as notes
        from int
        where src_sys_key = 'sfc'
        group by all
    ) sfc on int.hash_link = sfc.hash_link 
    left join departments on int.department_id = departments.intacct_id
    left join locations on int.location_id = locations.intacct_id and locations.id != '55-1'
    left join locations_intacct on int.location_key = locations_intacct.recordno
    left join entities on ifnull(locations_intacct.parentkey,int.location_key) = entities.id
    left join practice_areas on int.department_id = practice_areas.intacct_id
    left join project on int.hash_key_project = project.hash_key
    left join employee_int on int.employee_id_intacct = employee_int.intacct_employee_id
    left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link
    left join vw_employee_pay pay on employee_ukg.key = pay.key_employee and int.dte_entry between pay.date_from and pay.date_to
    left join currency_conversion as cc_pay_to_project on (
                                                                upper(currency_code_employee_original) = cc_pay_to_project.frm_curr 
                                                                and cc_pay_to_project.to_curr = currency_code_project
                                                                and cc_pay_to_project.date = pay.dte_job_effective
                                                            )
    left join currency_conversion as cc_bill_to_original on (
                                                                upper(currency_code_project) = cc_bill_to_original.frm_curr 
                                                                and cc_bill_to_original.to_curr = currency_code_employee_original
                                                                and cc_bill_to_original.date = int.dte_entry
                                                            )
    left join currency_conversion as cc_bill_to_entity on (
                                                                upper(currency_code_project) = cc_bill_to_entity.frm_curr 
                                                                and cc_bill_to_entity.to_curr = currency_code_employee_entity
                                                                and cc_bill_to_entity.date = int.dte_entry
                                                            )
    left join currency_conversion as cc_bill_to_usd on (
                                                                upper(currency_code_project) = cc_bill_to_usd.frm_curr 
                                                                and cc_bill_to_usd.to_curr = 'USD'
                                                                and cc_bill_to_usd.date = int.dte_entry
                                                            )

    --*****************************************************************************************************************************
    -- because we dont have the intacct rate ids, we have to default the conversion date to whatever the entry date is...
    left join currency_conversion as cc_ic_to_entity on (
                                                                upper(currency_code_employee_original) = cc_ic_to_entity.frm_curr 
                                                                and cc_ic_to_entity.to_curr = currency_code_employee_entity
                                                                and cc_ic_to_entity.date = int.dte_entry
                                                            )
    left join currency_conversion as cc_ic_to_usd on (
                                                                upper(currency_code_employee_original) = cc_ic_to_usd.frm_curr 
                                                                and cc_ic_to_usd.to_curr = 'USD'
                                                                and cc_ic_to_usd.date = int.dte_entry
                                                            )
    left join currency_conversion as cc_ic_to_project on (
                                                                upper(currency_code_employee_original) = cc_ic_to_project.frm_curr 
                                                                and cc_ic_to_project.to_curr = currency_code_project
                                                                and cc_ic_to_project.date = int.dte_entry
                                                            )
    where int.src_sys_key = 'int'