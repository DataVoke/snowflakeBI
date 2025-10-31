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
    silver_pbe as (
        Select * from {{ ref('sales_price_book_entry') }} where src_sys_key = 'sfc'
    ),
    silver_product as (
        Select * from {{ ref('sales_product') }} where src_sys_key = 'sfc'
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
        silver_oppty.key_account as key_account,
        por_continents.record_id as key_continent,
        por_countries.record_id as key_country,
        por_locations.record_id as key_location,
        por_entities.record_id as key_entity,
        silver_oppty_li.key_opportunity as key_opportunity,
        silver_oppty.key_parent_opportunity as key_parent_opportunity,
        coalesce(por_practices.record_id, oppty_li_practice.record_id, oppty_practice.record_id) as key_practice,
        por_practice_areas.record_id as key_practice_area,
        silver_oppty_li.key_product as key_product,
        int_project.key as key_project,
        silver_oppty_li.key_price_book_entry as key_price_book_entry,
        por_regions.record_id as key_region,
        por_states.record_id as key_state,
        silver_oppty_li.src_created_by_id as src_created_by_id,
        silver_oppty_li.src_modified_by_id as src_modified_by_id,
        silver_acct.name as account_name,
        silver_oppty_li.business_area,
        por_continents.display_name as continent_name,
        por_countries.display_name as country_name,
        silver_oppty_li.currency_iso_code,
        silver_oppty_li.division as division,
        silver_oppty_li.dts_src_created as dts_src_created,
        silver_oppty_li.dts_src_modified,
        silver_oppty_li.dts_system_modstamp as dts_system_modstamp,
        por_entities.display_name as entity_id,
        por_entities.name as entity_name,
        silver_oppty_li.list_price as list_price,
        por_locations.display_name as location_name,
        silver_oppty_li.name as name,
        silver_oppty.name as opportunity_name,
        parent_oppty.name as parent_opportunity_name,
        coalesce(por_practices.display_name, oppty_li_practice.display_name, oppty_practice.display_name) as practice_name,
        por_practice_areas.display_name as practice_area_name,
        silver_pbe.name as price_book_entry_name,
        silver_product.name as product_name,
        int_project.project_id as project_id,
        int_project.project_name,
        ifnull(silver_oppty_li.qty,0) as qty,
        por_regions.display_name as region_name,
        silver_oppty_li.sort_order,
        ifnull(silver_oppty_li.subtotal, 0) as subtotal,
        ifnull(silver_oppty_li.total_price, 0) as total_price,
        ifnull(silver_oppty_li.total_price_location, 0) as total_price_location,
        ifnull(silver_oppty_li.total_price_opportunity, 0) as total_price_opportunity,
        ifnull(silver_oppty_li.unit_price, 0) as unit_price
    from silver_oppty_li
    left join silver_oppty as silver_oppty on silver_oppty_li.key_opportunity = silver_oppty.key
    left join silver_oppty as parent_oppty on silver_oppty.key_parent_opportunity = parent_oppty.key
    left join silver_acct on silver_oppty.key_account = silver_acct.key
    left join int_project on silver_oppty.key_project = int_project.sfc_project_id
    left join por_practices as oppty_practice on silver_oppty.key_practice = oppty_practice.salesforce_id
    left join por_practices as oppty_li_practice on silver_oppty_li.key_practice = oppty_li_practice.salesforce_id
    left join por_practice_areas on silver_oppty_li.key_practice_area = por_practice_areas.salesforce_id
    left join por_practices on por_practice_areas.practice_id = por_practices.id
    left join por_countries on silver_acct.billing_country_code = por_countries.salesforce_id
    left join por_states on silver_acct.billing_state_code = por_states.salesforce_id and por_countries.id = por_states.country_id
    left join por_locations as por_locations on silver_acct.key_location = por_locations.salesforce_id
    left join por_regions as por_regions on por_locations.region_id = por_regions.id
    left join por_continents as por_continents on por_regions.continent_id = por_continents.id
    left join por_entities as por_entities on por_locations.entity_id = por_entities.id
    left join silver_pbe on silver_oppty_li.key_price_book_entry = silver_pbe.key
    left join silver_product on silver_oppty_li.key_product = silver_product.key