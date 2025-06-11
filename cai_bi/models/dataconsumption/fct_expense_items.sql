{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="expense_items"
    )
}}
with expense_item_int as (
    select * from {{ ref('expenseitem') }}
    where src_sys_key = 'int'
),

expense_item_sfc as (
    select * from {{ ref('expenseitem') }}
    where src_sys_key = 'sfc'
),

expense_int as (
    select * from {{ ref('expense') }}
    where src_sys_key = 'int'
),

expense_sfc as (
    select * from {{ ref('expense') }}
    where src_sys_key = 'sfc'
),

departments as (
    select * from {{ source('portal', 'departments') }} where _fivetran_deleted = false
),

locations as (
    select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false
    and id != '55-1'
),

entities as (
    select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false
),

locations_intacct as (
    select * from {{ source('sage_intacct', 'location') }} where _fivetran_deleted = false
),

final as (
    select
        int_ei.key,
        int_ei.key_expense,
        int_ei.key_employeee,
        int_ei.key_project,
        por_dep.record_id as key_department,
        por_loc.record_id as key_location,
        por_ent.record_id as key_entity,
        por_dep.display_name as department_name,
        por_loc.display_name as location_name,
        por_ent.display_name as entity_name,
        int_ei.account_key,
        int_ei.account_label_key,
        int_ei.customer_key,
        int_ei.customer_id,
        int_ei.detail_key,
        int_ei.employee_id,
        int_ei.expense_item_id,
        int_ei.exp_pmt_type_key,
        int_ei.item_key,
        int_ei.item_id,
        sfc_ei.milestone_id,
        int_ei.project_id,
        int_ei.src_created_by,
        int_ei.src_modified_by,
        int_ei.vendor_key,
        int_ei.vendor_id,
        int_e.approver_id,
        int_e.assignment_id,
        sfc_e.contact_id,
        coalesce(nullif(locations_intacct.parentid, ''), int_ei.location_id) as entity_id,
        int_e.record_id,
        int_e.tax_solution_id,
        int_ei.account_label,
        int_ei.account_no,
        int_ei.amt,
        -- ... rest of the columns ...
        int_e.total_trx_entered,
        int_e.total_trx_paid
    from expense_item_int int_ei
    left join expense_item_sfc sfc_ei
        on int_ei.hash_link = sfc_ei.hash_link
    left join expense_int int_e
        on int_ei.key_expense = int_e.key
    left join expense_sfc sfc_e
        on int_e.hash_link = sfc_e.hash_link
    left join departments por_dep
        on int_ei.department_id = por_dep.intacct_id
    left join locations por_loc
        on por_loc.intacct_id = int_ei.location_id
    left join entities por_ent
        on por_loc.entity_id = por_ent.id
    left join locations_intacct
        on int_ei.location_id = cast(locations_intacct.recordno as string)
    where int_e.key is not null
      and int_ei.line_item = true
)

select * from final
