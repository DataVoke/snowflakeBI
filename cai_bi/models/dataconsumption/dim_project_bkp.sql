{{ config(
    materialized = "table",
    schema = "dataconsumption",
    alias="project_bkp"
) }}

with

int as (
    select * from {{ ref('project') }} where src_sys_key = 'int'
),

pts as (
    select * from {{ ref('project') }} where src_sys_key = 'psa'
),

sfc as (
    select * from {{ ref('project') }} where src_sys_key = 'sfc'
),
employee_int as (
    select * from {{ ref('employee') }} where src_sys_key = 'int'
),
employee_sfc as (
    select * from {{ ref('employee') }} where src_sys_key = 'sfc'
),
employee_ukg as (
    select * from {{ ref('employee') }} where src_sys_key = 'ukg'
),

por_dep as (
    select * from {{ source('portal','departments') }} where _fivetran_deleted = false
),

por_grp as (
    select * from {{ source('portal','entities') }} where _fivetran_deleted = false
),

por_pract as (
    select * from {{ source('portal','practices') }} where _fivetran_deleted = false
),

por_pract_area as (
    select * from {{ source('portal','practice_areas') }} where _fivetran_deleted = false
),

por_loc as (
    select * from {{ source('portal','locations') }} where _fivetran_deleted = false and id != '55-1'
),

por_ent as (
    select * from {{ source('portal','entities') }} where _fivetran_deleted = false
),

locations_intacct as (
    select * from {{ source('sage_intacct','location') }} where _fivetran_deleted = false
),
forex_filtered as ( select * from {{ ref('ref_fx_rates_timeseries')}} where to_curr = 'USD' )

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,

    int.key,
    sfc.account_id,
    pts.assistant_project_manager_id,
    int.billto_key,
    int.client_site_id,
    sfc.client_manager_id,
    sfc.client_manager_name as client_manager_name,
    client_manager_ukg.display_name_lf as client_manager_name_lf,
    sfc.client_manager_email,
    int.contact_key,
    int.customer_id,
    int.customer_key,
    int.department_id,
    pts.estimate_id,
    sfc.group_id,
    int.location_id,
    int.location_id_intacct,
    int.manager_key,
    ifnull(nullif(locations_intacct.parentid, ''), int.location_id) as entity_id,
    sfc.opportunity_id,
    sfc.owner_id,
    int.parent_key,
    pts.portal_project_id,
    sfc.practice_id_intacct,
    int.project_dept_key,
    int.project_id,
    int.project_location_key,
    sfc.project_manager_id,
    employee_ukg.email_address_work,
    employee_ukg.email_address_personal,
    int.project_type_key,
    sfc.purchase_order_id,
    int.root_parent_id,
    int.root_parent_key,
    int.ship_to_key,
    int.src_created_by_id,
    int.src_modified_by_id,
    int.term_key,

    por_dep.record_id as key_department,
    por_loc.record_id as key_location,
    por_ent.record_id as key_entity,
    por_grp.record_id as key_group,
    por_pract.record_id as key_practice,
    employee_ukg.key as key_project_manager,
    por_pract_area.record_id as key_practice_area,

    por_dep.display_name as department_name,
    por_loc.display_name as location_name,
    por_ent.display_name as entity_name,
    por_grp.display_name as group_name,
    por_pract.display_name as practice_name,
    employee_ukg.display_name as project_manager_name,
    employee_ukg.display_name_lf as project_manager_name_lf,
    por_pract_area.display_name as practice_area_name,

    int.qty_actual,
    int.amt_total_billable,
    int.amt_total_budget,
    pts.assistant_project_manager_name,
    int.billing_over_max,
    int.billing_type,
    sfc.bln_allow_expenses_without_assignments,
    sfc.bln_allow_time_without_assignments,
    sfc.bln_daily_timecard_notes_required,
    sfc.bln_exclude_from_project_planner,
    sfc.bln_is_active,
    sfc.bln_is_billable,
    int.bln_is_billable_ap_po,
    int.bln_is_billable_expense,
    sfc.bln_pse_closed_for_expense_entry,
    sfc.bln_pse_closed_for_time_entry,
    pts.bln_top_concern,
    pts.bln_travel_prohibited,
    pts.concern_type,
    int.contact_name,
    sfc.contract_type,
    sfc.cost_expense,
    int.currency_iso_code,
    int.customer_name,
    int.dte_src_end,
    int.dte_src_start,
    pts.dts_int_last_assignment_sync,
    pts.dts_int_last_phase_code_sync,
    pts.dts_int_last_project_sync,
    pts.dts_last_resource_plan_review,
    pts.dts_pmo_data_migration,
    pts.dts_sfc_last_assignment_sync,
    pts.dts_sfc_last_phase_code_sync,
    pts.dts_sfc_last_project_sync,
    pts.dts_sfc_last_task_sync,
    int.dts_src_created,
    int.dts_src_modified,
    --sfc.group_name,
    int.invoice_currency,
    pts.last_resource_plan_review_by,
    pts.last_synced_status,
    int.memo,
    int.parent_name,
    pts.pmo_comments,
    pts.pnm_notes,
    pts.pnm_revision,
    coalesce(ex.fx_rate_div,1) as rate_div,
    coalesce(ex.fx_rate_mul,1) as rate_mul,
    coalesce(round(int.amt_po,2),0) as amt_po ,
    coalesce(round(case when rate_div >=0.09 then int.amt_po/rate_div else int.amt_po * rate_mul end,2),0) as amt_po_usd,
    int.po_number,
    pts.portal_project_code,
    int.project_category,
    int.project_description,
    int.project_name,
    int.project_status,
    int.project_type,
    pts.risk_rating,
    int.root_parent_name,
    pts.sharepoint_url,
    int.status,
    int.term_name,
    sfc.total_earned_value,
    sfc.total_number_of_tasks,
    pts.travel_rate

from int
left join pts on int.hash_link = pts.hash_link
left join sfc on sfc.hash_link = pts.hash_link
left join employee_int on int.project_manager_id = employee_int.intacct_employee_id
left join employee_sfc on sfc.client_manager_id = employee_sfc.key
left join employee_ukg on employee_int.hash_link = employee_ukg.hash_link
left join employee_ukg client_manager_ukg on employee_sfc.hash_link = client_manager_ukg.hash_link
left join por_dep on int.department_id = por_dep.intacct_id
left join por_grp on sfc.group_id = por_grp.salesforce_id
left join por_pract on por_pract.salesforce_id = sfc.practice_id
left join por_pract_area on int.department_id = por_pract_area.intacct_id
left join por_loc on por_loc.intacct_id = int.location_id
left join locations_intacct on int.project_location_key = locations_intacct.recordno
left join por_ent on coalesce(locations_intacct.parentkey,int.project_location_key) = por_ent.id
--left join por_ent on por_loc.entity_id = por_ent.id
left join forex_filtered ex on (int.currency_iso_code = ex.frm_curr )
        and ex.to_curr = 'USD'
        and ex.date = date(int.dts_src_created)
where lower(int.project_type) <> 'client site'
--qualify row_number() over ( partition by int.key order by ex.date desc ) =1
