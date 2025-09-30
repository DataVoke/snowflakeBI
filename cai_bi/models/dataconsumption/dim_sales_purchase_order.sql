{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_purchase_order"
    )
}}

with 
    po as (
        select * from {{ ref('sales_purchase_order') }} where src_sys_key = 'sfc'
    ),
    proposal as (
        select * from {{ ref('sales_proposal') }} where src_sys_key = 'sfc'
    ),
    opportunity as (
        select * from {{ ref('sales_opportunity') }} where src_sys_key = 'sfc'
    ),
    account as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc'
    ),
    project as (
        select * from {{ ref('project') }}
    ),
    por_locations as (
        select l.*, e.currency_id as entity_currency_id, e.record_id as entity_record_id, e.display_name as entity_display_name, r.record_id as region_record_id, r.display_name as region_display_name
        from {{ source('portal', 'locations') }} l
        left join {{ source('portal', 'location_regions') }} r on l.region_id = r.id
        left join {{ source('portal', 'entities') }} e on l.entity_id = e.id  
        where l._fivetran_deleted = false
    ),
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
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    po.key,
    proposal.key as key_proposal,
    opportunity.key as key_opportunity,
    opportunity.key_parent_opportunity as key_parent_opportunity,
    por_locations.entity_record_id as key_entity,
    por_locations.region_record_id as key_region,
    por_locations.record_id as key_location,
    int_project.key as key_project,
    account.key as key_account,
    sfc_project.key as sfc_project_id,
    po.amount as amt,
    ifnull(po.amount,0) * ifnull(cc_to_usd.fx_rate_mul,1) as amt_usd,
    ifnull(po.amount,0) * ifnull(cc_to_opportunity.fx_rate_mul,1) as amt_opportunity,
    ifnull(po.amount,0) * ifnull(cc_to_entity.fx_rate_mul,1) as amt_entity,
    po.currency_iso_code,
    'USD' as currency_iso_code_usd,
    opportunity.currency_iso_code as currency_iso_code_opportunity,
    por_locations.entity_currency_id as currency_iso_code_entity,
    po.dte,
    po.dts_src_created,
    po.dts_src_modified,
    po.dts_system_modstamp,
    po.name,
    po.notes,
    po.purchase_order_number,
    po.title,
    po.type,
    proposal.name as proposal_name,
    opportunity.name as opportunity_name,
    parent_opportunity.name as parent_opportunity_name, 
    account.name as account_name,
    por_locations.entity_display_name as entity_name,
    por_locations.region_display_name as region_name,
    por_locations.display_name as location_name,
    int_project.project_id,
    int_project.project_name
from po
left join proposal on po.hash_key_proposal = proposal.hash_key
left join opportunity on proposal.hash_key_opportunity = opportunity.hash_key
left join account on opportunity.hash_key_account = account.hash_key
left join opportunity as parent_opportunity on opportunity.hash_key_parent_opportunity = parent_opportunity.hash_key
left join project as sfc_project on opportunity.hash_key_project = sfc_project.hash_key and sfc_project.src_sys_key = 'sfc'
left join project as int_project on sfc_project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'
left join por_locations on account.key_location = por_locations.salesforce_id
left join currency_conversion as cc_to_usd on (
                    upper(po.currency_iso_code) = cc_to_usd.frm_curr 
                    and cc_to_usd.to_curr = 'USD'
                    and cc_to_usd.date = dte)  
left join currency_conversion as cc_to_opportunity on (
                    upper(po.currency_iso_code) = cc_to_opportunity.frm_curr 
                    and cc_to_opportunity.to_curr = opportunity.currency_iso_code
                    and cc_to_opportunity.date = dte)  
left join currency_conversion as cc_to_entity on (
                    upper(po.currency_iso_code) = cc_to_entity.frm_curr 
                    and cc_to_entity.to_curr = por_locations.entity_currency_id
                    and cc_to_entity.date = dte) 