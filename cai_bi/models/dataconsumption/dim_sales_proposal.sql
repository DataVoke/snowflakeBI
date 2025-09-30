{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_proposal"
    )
}}

with
    proposal as (select * from {{ ref('sales_proposal') }} where src_sys_key='sfc'),
    opportunity as (select * from {{ ref('sales_opportunity') }} where src_sys_key='sfc'),
    account as (select * from {{ ref('sales_account') }} where src_sys_key='sfc'),
    products as (select * from {{ ref('sales_product') }} where src_sys_key='sfc'),
    contact as (select * from {{ ref('sales_contact') }} where src_sys_key='sfc'),
    project as (select * from {{ ref('project') }}),
    por_practices as (select * from {{ source('portal', 'practices') }} where _fivetran_deleted=false),
    proposal_discounts as (
        select p.key as key_proposal,
            ifnull(sum(li.total_price_location),0) as amt_total_price_location, 
            ifnull(sum(li.total_price_opportunity),0) as amt_total_price_opportunity, 
            ifnull(sum(li.total_price),0) as amt_total_price, 
            ifnull(sum(li.total_price_location),0) - ifnull(sum(li.total_price),0) as amt_discount_location,
            ifnull(sum(li.total_price_opportunity),0) - ifnull(sum(li.total_price),0)  as amt_discount_opportunity,
            case when ifnull(sum(li.total_price),0) = 0 then 0 else (ifnull(sum(li.total_price),0) - ifnull(sum(li.total_price_location),0)) / ifnull(sum(unit_price),0) end as perc_discount_location,
            case when ifnull(sum(li.total_price),0) = 0 then 0 else (ifnull(sum(li.total_price),0) - ifnull(sum(li.total_price_location),0)) / ifnull(sum(unit_price),0) end as perc_discount_opportunity
        from {{ ref('sales_proposal_line_item') }} li
        inner join proposal p on li.key_proposal = p.key
        group by all
    ),
    ukg_employees as (
        select ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id, ifnull(sfc.key, por.contact_id) as sfc_contact_id, ukg.* 
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }}  as sfc on ukg.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }}  as por on ukg.hash_link = por.hash_link and por.src_sys_key = 'por'
        where ukg.src_sys_key = 'ukg'
    ),
    sfc_employees as (
        select sfc.salesforce_user_id as sfc_user_id, sfc.key as sfc_contact_id, sfc.* 
        from {{ ref('employee') }}  sfc
        where sfc.src_sys_key = 'sfc' and sfc.key not in (select sfc_contact_id from ukg_employees where sfc_contact_id is not null) 
    ),
    all_employees as (
        select * from ukg_employees 
        union
        select * from sfc_employees
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
    psa_estimates as (
        select * from {{ source('psatools', 'estimate') }} where _fivetran_deleted = false
    )
    
    
select
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    proposal.key,
    proposal.key_account_id as key_account,
    account_owner.key as key_account_owner,
    approver_1.key as key_approver_1,
    approver_2.key as key_approver_2,
    approver_3.key as key_approver_3,
    approver_4.key as key_approver_4,
    approver_5.key as key_approver_5,
    approver_6.key as key_approver_6,
    approver_7.key as key_approver_7,
    approver_8.key as key_approver_8,
    approver_9.key as key_approver_9,
    approver_10.key as key_approver_10,
    contact.key as key_contact,
    por_entities.record_id as key_entity,
    por_locations.record_id as key_location,
    por_regions.record_id as key_region,
    por_continents.record_id as key_continent,
    por_states.record_id as key_state,
    por_countries.record_id as key_country,
    proposal.key_opportunity,
    owner.key as key_owner,
    opportunity.key_parent_opportunity as key_parent_opportunity,
    ifnull(por_practices.record_id, por_practices_account.record_id) as key_practice,
    proposal.key_pricebook,
    int_project.key as key_project,
    por_regions.ukg_regional_manager_id as key_regional_manager,
    reviewer_1.key as key_reviewer_1,
    reviewer_2.key as key_reviewer_2,
    reviewer_3.key as key_reviewer_3,
    reviewer_4.key as key_reviewer_4,
    reviewer_5.key as key_reviewer_5,
    vice_president.key as key_vice_president,
    proposal.estimate_id,
    proposal.src_created_by_id,
    proposal.src_modified_by_id,
    proposal.amt_grand_total,
    proposal.amt_po_awarded,
    ifnull(proposal_discounts.amt_total_price_opportunity,0) as amt_total_price_opportunity,
    ifnull(proposal_discounts.amt_discount_opportunity,0) as amt_discount_opportunity,
    ifnull(proposal_discounts.perc_discount_opportunity,0) as perc_discount_opportunity, 
    ifnull(proposal_discounts.amt_total_price_location,0) as amt_total_price_location,
    ifnull(proposal_discounts.amt_discount_location,0) as amt_discount_location,
    ifnull(proposal_discounts.perc_discount_location,0) as perc_discount_location,
    proposal.bln_is_syncing,
    proposal.business_areas as business_area,
    proposal.currency_iso_code,
    proposal.description,
    proposal.dts_src_created,
    proposal.dts_src_modified,
    proposal.dts_system_modstamp,
    proposal.dte_internal_approval,
    proposal.name,
    proposal.pst_assist,
    proposal.quote_number,
    proposal.status,
    proposal.type_of_request,
    account.name as account_name,
    account_owner.display_name as account_owner_name,
    account_owner.display_name_lf as account_owner_name_lf,
    account_owner.email_address_work as account_owner_email,
    approver_1.display_name as approver_1_name,
    approver_1.display_name_lf as approver_1_name_lf,
    approver_1.email_address_work as approver_1_email,
    approver_2.display_name as approver_2_name,
    approver_2.display_name_lf as approver_2_name_lf,
    approver_2.email_address_work as approver_2_email,
    approver_3.display_name as approver_3_name,
    approver_3.display_name_lf as approver_3_name_lf,
    approver_3.email_address_work as approver_3_email,
    approver_4.display_name as approver_4_name,
    approver_4.display_name_lf as approver_4_name_lf,
    approver_4.email_address_work as approver_4_email,
    approver_5.display_name as approver_5_name,
    approver_5.display_name_lf as approver_5_name_lf,
    approver_5.email_address_work as approver_5_email,
    approver_6.display_name as approver_6_name,
    approver_6.display_name_lf as approver_6_name_lf,
    approver_6.email_address_work as approver_6_email,
    approver_7.display_name as approver_7_name,
    approver_7.display_name_lf as approver_7_name_lf,
    approver_7.email_address_work as approver_7_email,
    approver_8.display_name as approver_8_name,
    approver_8.display_name_lf as approver_8_name_lf,
    approver_8.email_address_work as approver_8_email,
    approver_9.display_name as approver_9_name,
    approver_9.display_name_lf as approver_9_name_lf,
    approver_9.email_address_work as approver_9_email,
    approver_10.display_name as approver_10_name,
    approver_10.display_name_lf as approver_10_name_lf,
    approver_10.email_address_work as approver_10_email,
    initcap(trim(concat(ifnull(contact.first_name,''), ' ', ifnull(contact.last_name,'')))) as contact_name,
    initcap(trim(concat(ifnull(contact.last_name,''), ', ', ifnull(contact.first_name,'')))) as contact_name_lf,
    contact.email as contact_email,
    por_entities.display_name as entity_name,
    por_locations.display_name as location_name,
    por_regions.display_name as region_name,
    por_continents.display_name as continent_name,
    por_states.display_name as state_name,
    por_countries.display_name as country_name,
    psa_estimates.name as estimate_name,
    opportunity.name as opportunity_name,
    opportunity.stage_name as opportunity_stage_name,
    owner.display_name as owner_name,
    owner.display_name_lf as owner_name_lf,
    owner.email_address_work as owner_email,
    parent_opportunity.name as parent_opportunity_name,
     ifnull(por_practices.display_name, por_practices_account.display_name) as practice_name,
    int_project.project_id,
    int_project.project_name,
    regional_manager.display_name as regional_manager_name,
    regional_manager.display_name_lf as regional_manager_name_lf,
    regional_manager.email_address_work as regional_manager_email,
    reviewer_1.display_name as reviewer_1_name,
    reviewer_1.display_name_lf as reviewer_1_name_lf,
    reviewer_1.email_address_work as reviewer_1_email,
    reviewer_2.display_name as reviewer_2_name,
    reviewer_2.display_name_lf as reviewer_2_name_lf,
    reviewer_2.email_address_work as reviewer_2_email,
    reviewer_3.display_name as reviewer_3_name,
    reviewer_3.display_name_lf as reviewer_3_name_lf,
    reviewer_3.email_address_work as reviewer_3_email,
    reviewer_4.display_name as reviewer_4_name,
    reviewer_4.display_name_lf as reviewer_4_name_lf,
    reviewer_4.email_address_work as reviewer_4_email,
    reviewer_5.display_name as reviewer_5_name,
    reviewer_5.display_name_lf as reviewer_5_name_lf,
    reviewer_5.email_address_work as reviewer_5_email,
    vice_president.display_name as vice_president_name,
    vice_president.display_name_lf as vice_president_name_lf,
    vice_president.email_address_work as vice_president_email,
    proposal.key_account_owner as sfc_account_owner_id,
    proposal.key_approver_1 as sfc_approver_1_id,
    proposal.key_approver_2 as sfc_approver_2_id,
    proposal.key_approver_3 as sfc_approver_3_id,
    proposal.key_approver_4 as sfc_approver_4_id,
    proposal.key_approver_5 as sfc_approver_5_id,
    proposal.key_approver_6 as sfc_approver_6_id,
    proposal.key_approver_7 as sfc_approver_7_id,
    proposal.key_approver_8 as sfc_approver_8_id,
    proposal.key_approver_9 as sfc_approver_9_id,
    proposal.key_approver_10 as sfc_approver_10_id,
    proposal.key_contact as sfc_contact_id,
    proposal.key_owner_id as sfc_owner_id,
    ifnull(opportunity.key_practice, account.key_practice) as sfc_practice_id,
    proposal.key_project as sfc_project_id,
    proposal.key_regional_manager as sfc_regional_manager_id,
    proposal.key_reviewer_1 as sfc_reviewer_1_id,
    proposal.key_reviewer_2 as sfc_reviewer_2_id,
    proposal.key_reviewer_3 as sfc_reviewer_3_id,
    proposal.key_reviewer_4 as sfc_reviewer_4_id,
    proposal.key_reviewer_5 as sfc_reviewer_5_id,
    proposal.key_vice_president as sfc_vice_president_id
from proposal
left join opportunity on proposal.key_opportunity = opportunity.key
left join opportunity as parent_opportunity on opportunity.key_parent_opportunity = parent_opportunity.key
left join account on opportunity.key_account = account.key
left join por_practices on opportunity.key_practice = por_practices.salesforce_id
left join por_practices as por_practices_account on account.key_practice = por_practices_account.salesforce_id
left join project on ifnull(opportunity.key_project, proposal.key_project) = project.key and project.src_sys_key='sfc'
left join project int_project on project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'
left join all_employees as account_owner on proposal.key_account_owner = account_owner.sfc_user_id
left join all_employees as approver_1 on proposal.key_approver_1 = approver_1.sfc_user_id
left join all_employees as approver_2 on proposal.key_approver_2 = approver_2.sfc_user_id
left join all_employees as approver_3 on proposal.key_approver_3 = approver_3.sfc_user_id
left join all_employees as approver_4 on proposal.key_approver_4 = approver_4.sfc_user_id
left join all_employees as approver_5 on proposal.key_approver_5 = approver_5.sfc_user_id
left join all_employees as approver_6 on proposal.key_approver_6 = approver_6.sfc_user_id
left join all_employees as approver_7 on proposal.key_approver_7 = approver_7.sfc_user_id
left join all_employees as approver_8 on proposal.key_approver_8 = approver_8.sfc_user_id
left join all_employees as approver_9 on proposal.key_approver_9 = approver_9.sfc_user_id
left join all_employees as approver_10 on proposal.key_approver_10 = approver_10.sfc_user_id
left join contact on proposal.key_contact = contact.key
left join all_employees as owner on proposal.key_owner_id = owner.sfc_user_id
left join por_countries on account.billing_country_code = por_countries.salesforce_id
left join por_states on account.billing_state_code = por_states.salesforce_id and por_countries.id = por_states.country_id
left join por_locations as por_locations on account.key_location = por_locations.salesforce_id
left join por_entities as por_entities on por_locations.entity_id = por_entities.id
left join por_regions as por_regions on por_locations.region_id = por_regions.id
left join por_continents as por_continents on por_regions.continent_id = por_continents.id
left join all_employees as regional_manager on por_regions.ukg_regional_manager_id = regional_manager.key
left join all_employees as reviewer_1 on proposal.key_reviewer_1 = reviewer_1.sfc_user_id
left join all_employees as reviewer_2 on proposal.key_reviewer_2 = reviewer_2.sfc_user_id
left join all_employees as reviewer_3 on proposal.key_reviewer_3 = reviewer_3.sfc_user_id
left join all_employees as reviewer_4 on proposal.key_reviewer_4 = reviewer_4.sfc_user_id
left join all_employees as reviewer_5 on proposal.key_reviewer_5 = reviewer_5.sfc_user_id
left join all_employees as vice_president on proposal.key_vice_president = vice_president.sfc_user_id
left join proposal_discounts on proposal.key = proposal_discounts.key_proposal
left join psa_estimates on proposal.estimate_id = psa_estimates.estimateid