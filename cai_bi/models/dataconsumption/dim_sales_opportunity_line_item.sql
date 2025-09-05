{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_opportunity_line_item"
    )
}}

with 
    silver_acct as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
    ),
    silver_oppty_li as (
        select * from {{ ref('sales_opportunity_line_item') }} where src_sys_key = 'sfc' 
    ),
    silver_oppty as (
        select * from {{ ref('sales_opportunity') }} where src_sys_key = 'sfc' 
    ),
    por_practices as (
        select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false
    ),
    por_practice_areas as (
        select * from {{ source('portal', 'practice_areas') }} where _fivetran_deleted = false
    ),
    por_locations as (
        select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false
    ),
    por_regions as (
        select u.ukg_id as ukg_regional_manager_id, r.* 
        from {{ source('portal', 'location_regions') }} r
        left join {{ source('portal', 'users') }} as u on r.regional_manager_user_id = u.id
        where r._fivetran_deleted = false
    ),
    por_continents as (
        select * from {{ source('portal', 'location_continents') }} where _fivetran_deleted = false
    ),
    por_states as (
        select * from {{ source('portal', 'states') }} where _fivetran_deleted = false
    ),
    por_countries as (
        select * from {{ source('portal', 'countries') }} where _fivetran_deleted = false
    ),
    por_entities as (
        select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false
    ),
    int_project as (
        select sfc.key as sfc_project_id, int.* 
        from {{ ref('project') }} int
        left join {{ ref('project') }}  as sfc on int.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
        where int.src_sys_key = 'int'
    )

select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
'{{ this.name }}' as created_by,
cast(current_timestamp as timestamp_tz) as dts_updated_at,
'{{ this.name }}' as updated_by,
silver_oppty_li.key as key,
silver_oppty_li.key as key_account,
silver_oppty_li.record_id,
silver_oppty_li.record_id,
silver_oppty_li.record_id as key_location,
silver_oppty_li.record_id as key_entity,
silver_oppty_li.key_opportunity as key_opportunity,
silver_oppty_li.key_parent_opportunity as key_parent_opportunity,
silver_oppty_li.record_id,
silver_oppty_li.record_id as key_practice_area,
silver_oppty_li. as key_product,
silver_oppty_li.key,
silver_oppty_li.key_price_book_entry,
silver_oppty_li.record_id as key_region,
silver_oppty_li.record_id,
silver_oppty_li.src_created_by_id as src_created_by_id,
silver_oppty_li.src_modified_by_id as src_modified_by_id,
silver_oppty_li.name,
silver_oppty_li.business_area,
silver_oppty_li.display_name,
silver_oppty_li.display_name,
silver_oppty_li.currency_iso_code,
silver_oppty_li.division as division,
silver_oppty_li.dts_src_created as dts_src_created,
silver_oppty_li.dts_src_modified,
silver_oppty_li.dts_system_modstamp as dts_system_modstamp,
silver_oppty_li.display_name,
silver_oppty_li.name as entity_name,
silver_oppty_li.list_price as list_price,
silver_oppty_li.display_name as location_name,
silver_oppty_li.name as name,
silver_oppty_li.name as opportunity_name,
silver_oppty_li.name as parent_opportunity_name,
silver_oppty_li.display_name,
silver_oppty_li.display_name,
silver_oppty_li.name,
silver_oppty_li.name,
silver_oppty_li.project_id as project_id,
silver_oppty_li.project_name,
silver_oppty_li.qty,
silver_oppty_li.display_name,
silver_oppty_li.sort_order,
silver_oppty_li.subtotal,
silver_oppty_li.total_price,
silver_oppty_li.total_price_location,
silver_oppty_li.total_price_opportunity,
silver_oppty_li.unit_price,

    --Fields
    
    silver_acct.name as account_name,
    nullif(ltrim(concat(ifnull(parent_account8.key,''),'|', ifnull(parent_account7.key,''), ifnull(parent_account6.key,''),'|', ifnull(parent_account5.key,''),'|', ifnull(parent_account4.key,''),'|', ifnull(parent_account3.key,''),'|', ifnull(parent_account2.key,''),'|', ifnull(parent_account1.key,'')),'|'),'') as account_hierarchy_path,
    coalesce(parent_account8.name, parent_account7.name, parent_account6.name, parent_account5.name, parent_account4.name, parent_account3.name, parent_account2.name, parent_account1.name) as top_level_parent_account_name,
    coalesce(silver_acct.grade, parent_account1.grade, parent_account2.grade, parent_account3.grade, parent_account4.grade, parent_account5.grade, parent_account6.grade, parent_account7.grade, parent_account8.grade) as account_grade,
    coalesce(silver_acct.payment_terms, parent_account1.payment_terms, parent_account2.payment_terms, parent_account3.payment_terms, parent_account4.payment_terms, parent_account5.payment_terms, parent_account6.payment_terms, parent_account7.payment_terms, parent_account8.payment_terms) as account_payment_terms,
    ifnull(por_practices.display_name,por_practices_account.display_name) as practice_name,
    por_states.display_name as state_name,
    por_countries.display_name as country_name,
    por_entities.display_name as entity_id,
    por_entities.name as entity_name,
    por_entities.ukg_id as company_id,
    por_entities.ukg_company_code as company_code,
    por_locations.display_name as location_name,
    por_regions.display_name as region_name,
    por_continents.display_name as continent_name,
    psa_estimates.name as estimate_name,
    emp_identified_by.display_name as identified_by_name,
    emp_identified_by.display_name_lf as identified_by_name_lf,
    emp_identified_by.email_address_work as identified_by_email,
    emp_rm.display_name as regional_manager_name,
    emp_rm.display_name_lf as regional_manager_name_lf,
    emp_rm.email_address_work as regional_manager_email,
    emp_owner.display_name as owner_name,
    emp_owner.display_name_lf as owner_name_lf,
    emp_owner.email_address_work as owner_email,
    emp_client_manager.display_name as client_site_manager_name,
    emp_client_manager.display_name_lf as client_site_manager_name_lf,
    emp_client_manager.email_address_work as client_site_manager_email,
    int_project.project_id,
    int_project.project_name,
    silver_rcs.name as rate_card_set_name
from silver_oppty_li
left join silver_oppty as silver_oppty on silver_oppty_li.key_opportunity = silver_oppty.key
left join silver_oppty as parent_oppty on silver_oppty.key_parent_opportunity = parent_oppty.key
left join silver_acct on silver_oppty.key_account = silver_acct.key
left join int_project on silver_oppty.key_project = int_project.sfc_project_id
left join por_practices on silver_oppty.key_practice = por_practices.salesforce_id
left join por_practices as por_practices_account on silver_acct.key_practice = por_practices_account.salesforce_id
left join por_countries on silver_acct.billing_country_code = por_countries.salesforce_id
left join por_states on silver_acct.billing_state_code = por_states.salesforce_id and por_countries.id = por_states.country_id
left join por_locations as por_locations on silver_acct.key_location = por_locations.salesforce_id
left join por_regions as por_regions on por_locations.region_id = por_regions.id
left join por_continents as por_continents on por_regions.continent_id = por_continents.id
left join por_entities as por_entities on por_locations.entity_id = por_entities.id
left join silver_acct parent_account1 on silver_acct.key_parent_account = parent_account1.key
left join silver_acct parent_account2 on parent_account1.key_parent_account = parent_account2.key
left join silver_acct parent_account3 on parent_account2.key_parent_account = parent_account3.key
left join silver_acct parent_account4 on parent_account3.key_parent_account = parent_account4.key
left join silver_acct parent_account5 on parent_account4.key_parent_account = parent_account5.key
left join silver_acct parent_account6 on parent_account5.key_parent_account = parent_account6.key
left join silver_acct parent_account7 on parent_account6.key_parent_account = parent_account7.key
left join silver_acct parent_account8 on parent_account7.key_parent_account = parent_account8.key
left join silver_acct parent_account9 on parent_account8.key_parent_account = parent_account9.key