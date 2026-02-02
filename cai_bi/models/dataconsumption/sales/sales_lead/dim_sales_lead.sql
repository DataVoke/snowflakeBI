{{ config(
    materialized = "table",
    schema = "dataconsumption",
    alias="sales_lead"
) }}

with
    sf_leads as (select * from {{ ref('sales_lead') }}),
    por_practices as (
        select * from {{ source('portal','practices') }} where _fivetran_deleted = false
    ),
    por_practice_areas as (
        select * from {{ source('portal','practice_areas') }} where _fivetran_deleted = false
    ),
    por_locations as (
        select * from {{ source('portal','locations') }} where _fivetran_deleted = false
    ),
    por_regions as (
        select u.ukg_id as ukg_regional_manager_id, r.* 
        from {{ source('portal','location_regions') }} r
        left join {{ source('portal','users') }} as u on r.regional_manager_user_id = u.id
        where r._fivetran_deleted = false
    ),
    por_continents as (
        select * from {{ source('portal','location_continents') }} where _fivetran_deleted = false
    ),
    por_states as (
        select * from {{ source('portal','states') }} where _fivetran_deleted = false
    ),
    por_countries as (
        select * from {{ source('portal','countries') }} where _fivetran_deleted = false
    ),
    por_entities as (
        select * from {{ source('portal','entities') }} where _fivetran_deleted = false
    ),
    ukg_employees as (
        select ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id, ifnull(sfc.key, por.contact_id) as sfc_contact_id, ukg.* 
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }} as sfc on ukg.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }} as por on ukg.hash_link = por.hash_link and por.src_sys_key = 'por'
        where ukg.src_sys_key = 'ukg'
    ),
    sfc_employees as (
        select sfc.salesforce_user_id as sfc_user_id, sfc.key as sfc_contact_id, sfc.* 
        from {{ ref('employee') }} sfc
        where sfc.src_sys_key = 'sfc' and sfc.key not in (select sfc_contact_id from ukg_employees where sfc_contact_id is not null) 
    ),
    sfc_users as (
        select * from {{ source("salesforce", "user") }} 
    ),
    all_employees as (
        select * from ukg_employees 
        union
        select * from sfc_employees
    )

select
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    sf_leads.key,
    por_continents.record_id as key_continent,
    por_countries.record_id as key_country,
    por_entities.record_id as key_entity,
    por_locations.record_id as key_location,
    lead_owner.key as key_owner,
    por_practices.record_id as key_practice,
    por_practice_areas.record_id as key_practice_area,
    por_regions.record_id as key_region,
    regional_managers.key as key_regional_manager,
    por_states.record_id as key_state,
    sf_leads.src_created_by_id,
    sf_leads.src_modified_by_id,
    sf_leads.key_owner as sfc_owner_id,
    sf_leads.converted_account_id,
    sf_leads.converted_contact_id,
    sf_leads.converted_opportunity_id,
    sf_leads.external_id,
    sf_leads.additional_business_areas,
    sf_leads.additional_lead_sources,
    sf_leads.amt_budget,
    sf_leads.bln_bypass_opportunity_validation,
    sf_leads.bln_convert,
    sf_leads.bln_currency_confirmed,
    sf_leads.bln_has_opted_out_of_email,
    sf_leads.bln_is_converted,
    sf_leads.bln_is_created_from_lead,
    sf_leads.bln_is_marketing_qualified_lead,
    sf_leads.bln_is_synced_to_marketo,
    sf_leads.bln_is_unread_by_owner,
    sf_leads.city,
    sf_leads.company,
    sf_leads.conferenceor_show,
    sf_leads.content_theme,
    sf_leads.content_title,
    sf_leads.country,
    sf_leads.country_code,
    sf_leads.currency_iso_code,
    sf_leads.data_center_primary_business_area,
    sf_leads.description,
    initcap(trim(concat(ifnull(sf_leads.first_name,''),' ',ifnull(sf_leads.last_name,'')))) as display_name,
    initcap(trim(concat(ifnull(sf_leads.last_name,''),', ',ifnull(sf_leads.first_name,'')))) as display_name_lf,
    sf_leads.division,
    sf_leads.dte_converted,
    sf_leads.dte_last_activity,
    sf_leads.dte_last_transfer,
    sf_leads.dts_email_bounced,
    sf_leads.dts_marketo_acquisition_date,
    sf_leads.dts_src_created,
    sf_leads.dts_src_modified,
    sf_leads.dts_system_modstamp,
    sf_leads.email,
    sf_leads.email_bounced_reason,
    sf_leads.fax,
    sf_leads.first_name,
    sf_leads.inbound_marketing_source,
    sf_leads.industry,
    sf_leads.last_name,
    sf_leads.lead_source,
    sf_leads.legal_basis_processing_data,
    sf_leads.lid_linked_in_member_token,
    sf_leads.life_sciences_primary_business_area,
    sf_leads.linked_in_url,
    sf_leads.marketo_acquisition_program,
    sf_leads.marketo_lead_score,
    sf_leads.middle_name,
    sf_leads.mobile_phone,
    sf_leads.name,
    sf_leads.number_of_employees,
    sf_leads.or_oe,
    sf_leads.phone,
    sf_leads.postal_code,
    sf_leads.primary_business_area,
    sf_leads.rating,
    sf_leads.reference_number,
    sf_leads.salutation,
    sf_leads.state,
    sf_leads.status,
    sf_leads.street,
    sf_leads.subtype,
    sf_leads.suffix,
    sf_leads.title,
    sf_leads.type,
    sf_leads.website,
    por_continents.display_name as continent_name,
    por_countries.display_name as country_name,
    por_entities.display_name as entity_name,
    por_locations.display_name as location_name,
    ifnull(lead_owner.display_name, sfc_users.name) as owner_name,
    ifnull(lead_owner.display_name_lf, sfc_users.name) as owner_name_lf,
    ifnull(lead_owner.email_address_work, sfc_users.email) as owner_name_email,
    por_practices.display_name as practice_name,
    por_practice_areas.display_name as practice_area_name,
    por_regions.display_name as region_name,
    regional_managers.display_name as regional_manager_name,
    regional_managers.display_name_lf as regional_manager_name_lf,
    regional_managers.email_address_work as regional_manager_name_email,
    por_states.display_name as state_name
from sf_leads
left join por_countries on sf_leads.country_code = por_countries.salesforce_id
left join por_states on sf_leads.state = por_states.name and por_countries.id = por_states.country_id
left join por_locations as por_locations on por_states.location_id = por_locations.id
left join por_regions as por_regions on por_locations.region_id = por_regions.id
left join por_continents as por_continents on por_regions.continent_id = por_continents.id
left join por_entities as por_entities on por_locations.entity_id = por_entities.id
left join por_practices on sf_leads.division = por_practices.name
left join por_practice_areas on sf_leads.primary_business_area = por_practice_areas.name
left join all_employees as lead_owner on sf_leads.key_owner = lead_owner.sfc_user_id
left join sfc_users on sf_leads.key_owner = sfc_users.id
left join all_employees as regional_managers on por_regions.ukg_regional_manager_id = regional_managers.key