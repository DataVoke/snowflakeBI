{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_proposal_line_item"
    )
}}

with
    proposal_li as (select * from {{ ref('sales_proposal_line_item') }} where src_sys_key='sfc'),
    proposal as (select * from {{ ref('sales_proposal') }} where src_sys_key='sfc'),
    opportunity as (select * from {{ ref('sales_opportunity') }} where src_sys_key='sfc'),
    account as (select * from {{ ref('sales_account') }} where src_sys_key='sfc'),
    products as (select * from {{ ref('sales_product') }} where src_sys_key='sfc'),
    price_book_entry as (select * from {{ ref('sales_price_book_entry') }} where src_sys_key='sfc'),
    project as (select * from {{ ref('project') }}),
    practice_areas as (select * from {{ source('portal', 'practice_areas') }} where _fivetran_deleted=false),
    practices as (select * from {{ source('portal', 'practices') }} where _fivetran_deleted=false)
    
select
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    proposal_li.key as key,
    opportunity.key_account,
    opportunity.key as key_oppportunity,
    proposal_li.key_opportunity_line_item,
    practices.record_id as key_practice,
    practice_areas.record_id as key_practice_area,
    opportunity.key_parent_opportunity,
    proposal_li.key_product,
    proposal_li.key_pricebook_entry as key_price_book_entry,
    int_project.key as key_project,
    proposal_li.src_created_by_id,
    proposal_li.src_modified_by_id,
    proposal_li.currency_iso_code,
    proposal_li.dts_src_created,
    proposal_li.dts_src_modified,
    proposal_li.dts_system_modstamp,
    proposal_li.list_price,
    proposal_li.qty,
    proposal_li.sort_order,
    proposal_li.subtotal,
    proposal_li.total_price,
    proposal_li.total_price_location,
    proposal_li.total_price_opportunity,
    proposal_li.unit_price,
    account.name as account_name,
    opportunity.name as opportunity_name,
    practices.display_name as practice_name,
    practice_areas.display_name as practice_area_name,
    products.name as product_name,
    price_book_entry.name as price_book_entry_name,
    parent_opportunity.name as parent_opportunity_name,
    int_project.project_id,
    proposal.status as proposal_status,
    opportunity.stage_name as opportunity_stage_name,
    ifnull(opportunity.key_project, proposal.key_project) as sfc_project_id
from proposal_li
left join proposal on proposal_li.key_proposal = proposal.key
left join opportunity on proposal.key_opportunity = opportunity.key
left join opportunity as parent_opportunity on opportunity.key_parent_opportunity = parent_opportunity.key
left join account on opportunity.key_account = account.key
left join practices on proposal_li.key_practice = practices.salesforce_id
left join practice_areas on proposal_li.key_practice_area = practice_areas.salesforce_id
left join products on proposal_li.key_product = products.key
left join price_book_entry on proposal_li.key_pricebook_entry = price_book_entry.key
left join project on ifnull(opportunity.key_project, proposal.key_project) = project.key and project.src_sys_key='sfc'
left join project int_project on project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'