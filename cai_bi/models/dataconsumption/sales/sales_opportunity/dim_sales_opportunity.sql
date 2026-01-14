{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="sales_opportunity"
    )
}}

with 
    silver_acct as (
        select * from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
    ),
    silver_oppty as (
        select * from {{ ref('sales_opportunity') }} where src_sys_key = 'sfc' 
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
    oppty_discounts as (
        select o.key as key_opportunity,
            ifnull(sum(li.total_price_location),0) as amt_total_price_location, 
            ifnull(sum(li.total_price_opportunity),0) as amt_total_price_opportunity, 
            ifnull(sum(li.total_price),0) as amt_total_price, 
            ifnull(sum(li.total_price_location),0) - ifnull(sum(li.total_price),0) as amt_discount_location,
            ifnull(sum(li.total_price_opportunity),0) - ifnull(sum(li.total_price),0)  as amt_discount_opportunity,
            case when ifnull(sum(li.total_price),0) = 0 then 0 else (ifnull(sum(li.total_price),0) - ifnull(sum(li.total_price_location),0)) / ifnull(sum(unit_price),0) end as perc_discount_location,
            case when ifnull(sum(li.total_price),0) = 0 then 0 else (ifnull(sum(li.total_price),0) - ifnull(sum(li.total_price_location),0)) / ifnull(sum(unit_price),0) end as perc_discount_opportunity
        from {{ ref('sales_opportunity_line_item') }} li
        inner join {{ ref('sales_opportunity') }} o on li.key_opportunity = o.key
        group by all
    ),
    all_employees as (
        select * from ukg_employees 
        union
        select * from sfc_employees
    ),
    silver_rcs as (
        select * from {{ ref('sales_rate_card_set') }} where src_sys_key = 'sfc'
    ),
    por_practices as (
        select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false
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
    silver_oppty.key,
    silver_oppty.key_account,
    emp_client_manager.key as key_client_site_manager,
    por_continents.record_id as key_continent,
    por_countries.record_id as key_country,
    por_entities.record_id as key_entity,
    silver_oppty.key_estimate,
    emp_identified_by.key as key_identified_by,
    por_locations.record_id as key_location,
    emp_owner.key as key_owner,
    silver_oppty.key_parent_opportunity as key_parent_opportunity,
    ifnull(por_practices.record_id, por_practices_account.record_id) as key_practice,
    ifnull(int_project.key, int_project_parent.key) as key_project,
    silver_oppty.key_proposal as key_proposal,
    silver_oppty.key_rate_card_set,
    por_regions.record_id as key_region,
    por_regions.ukg_regional_manager_id as key_regional_manager,
    por_states.record_id as key_state,
    coalesce(parent_account8.key,parent_account7.key,parent_account6.key,parent_account5.key,parent_account4.key,parent_account3.key,parent_account2.key,parent_account1.key) as key_top_level_parent_account,
    
    --Ids
    silver_oppty.campaign_id,
    silver_oppty.contact_id,
    silver_oppty.external_id,
    silver_oppty.last_amount_changed_history_id,
    silver_oppty.last_close_date_changed_history_id as last_close_date_changed_history_id,
    silver_oppty.key_price_book_entry as price_book_entry_id,
    silver_oppty.portal_project_id_c as portal_project_id_c,
    silver_oppty.src_created_by_id as src_created_by_id,
    silver_oppty.src_modified_by_id as src_modified_by_id,
    silver_oppty.key_location as sfc_location_id,
    coalesce(silver_oppty.key_project, parent_oppty.key_project) as sfc_project_id,
    ifnull(silver_oppty.key_practice, silver_acct.key_practice) as sfc_practice_id,
    silver_oppty.key_owner as sfc_owner_id,
    silver_oppty.key_identified_by as sfc_identified_by_id,

    --Fields
    silver_oppty.amt_child_opportunities,
    silver_oppty.amt_child_po_awarded,
    silver_oppty.amt_contingency_budget,
    silver_oppty.amt_expected_revenue,
    silver_oppty.amt_labor_budget as amt_labor_budget,
    silver_oppty.amt_other_budget as amt_other_budget,
    silver_oppty.amt_po_awarded as amt_po_awarded,
    silver_oppty.amt_subcontractor_budget as amt_subcontractor_budget,
    silver_oppty.amt as amt_total_price,
    oppty_discounts.amt_total_price_opportunity as amt_total_price_opportunity,
    oppty_discounts.amt_discount_opportunity as amt_discount_opportunity,
    oppty_discounts.perc_discount_opportunity as perc_discount_opportunity, 
    oppty_discounts.amt_total_price_location as amt_total_price_location,
    oppty_discounts.amt_discount_location as amt_discount_location,
    oppty_discounts.perc_discount_location as perc_discount_location, 
    silver_oppty.amt_travel_budget,
    silver_oppty.bln_budget_confirmed,
    silver_oppty.bln_currency_confirmed,
    silver_oppty.bln_has_bypass_po_or_loi,
    silver_oppty.bln_has_letter_of_intent_received,
    silver_oppty.bln_has_open_activity,
    silver_oppty.bln_has_opportunity_line_item,
    silver_oppty.bln_has_overdue_task,
    silver_oppty.bln_has_signed_purchase_order,
    silver_oppty.bln_has_signed_sla,
    silver_oppty.bln_is_change_request,
    silver_oppty.bln_is_closed,
    silver_oppty.bln_is_converted,
    silver_oppty.bln_is_covid_19_vaccine,
    silver_oppty.bln_is_created_from_lead,
    silver_oppty.bln_is_mvp,
    silver_oppty.bln_is_on_hold,
    silver_oppty.bln_is_won,
    silver_oppty.bv_cai_proposal_no,
    silver_oppty.cai_team_engaged_in_pursuit,
    silver_oppty.closed_won_reason,
    silver_oppty.contract_type,
    silver_oppty.currency_iso_code,
    silver_oppty.description,
    silver_oppty.dte_close,
    silver_oppty.dte_customer_po_awarded,
    silver_oppty.dte_estimated_project_end,
    silver_oppty.dte_estimated_project_start,
    silver_oppty.dte_last_activity,
    silver_oppty.dte_proposal_submitted,
    silver_oppty.dte_stage_last_updated,
    silver_oppty.dts_last_stage_change,
    silver_oppty.dts_src_created,
    silver_oppty.dts_src_modified,
    silver_oppty.dts_system_modstamp,
    silver_oppty.duration_closed_won,
    silver_oppty.duration_interview_presentation,
    silver_oppty.duration_negotiation_review,
    silver_oppty.duration_proposal_approved,
    silver_oppty.duration_proposal_development,
    silver_oppty.duration_proposal_submitted,
    silver_oppty.duration_prospecting_qualification,
    silver_oppty.duration_solution_development,
    silver_oppty.fiscal,
    silver_oppty.fiscal_quarter,
    silver_oppty.fiscal_year,
    silver_oppty.forecast_category,
    silver_oppty.forecast_category_name,
    silver_oppty.kare,
    silver_oppty.lead_source,
    silver_oppty.loss_reason_c,
    silver_oppty.loss_reason_description_c,
    silver_oppty.name,
    parent_oppty.name as parent_opportunity_name,
    silver_oppty.next_step,
    silver_oppty.or_oe_c,
    silver_oppty.portal_project_code_c,
    silver_oppty.probability,
    silver_oppty.scope_type_c,
    silver_oppty.stage_name,
    silver_oppty.subtype_c,
    silver_oppty.total_opportunity_quantity,
    silver_oppty.type_c,
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
    ifnull(int_project.project_id,int_project_parent.project_id) as project_id,
    ifnull(int_project.project_name, int_project_parent.project_name) as project_name,
    silver_rcs.name as rate_card_set_name
from silver_oppty
left join silver_oppty as parent_oppty on silver_oppty.key_parent_opportunity = parent_oppty.key
left join silver_acct on silver_oppty.key_account = silver_acct.key
left join oppty_discounts on silver_oppty.key = oppty_discounts.key_opportunity
left join psa_estimates on silver_oppty.key_estimate = psa_estimates.estimateid
left join all_employees as emp_identified_by on silver_oppty.key_identified_by = emp_identified_by.sfc_contact_id
left join all_employees as emp_owner on silver_oppty.key_owner = emp_owner.sfc_user_id
left join all_employees as emp_client_manager on silver_acct.key_client_manager = emp_client_manager.sfc_contact_id
left join int_project on silver_oppty.key_project = int_project.sfc_project_id
left join int_project as int_project_parent on parent_oppty.key_project = int_project_parent.sfc_project_id
left join silver_rcs on silver_oppty.key_rate_card_set = silver_rcs.key
left join por_practices on silver_oppty.key_practice = por_practices.salesforce_id
left join por_practices as por_practices_account on silver_acct.key_practice = por_practices_account.salesforce_id
left join por_countries on silver_acct.billing_country_code = por_countries.salesforce_id
left join por_states on silver_acct.billing_state_code = por_states.salesforce_id and por_countries.id = por_states.country_id
left join por_locations as por_locations on silver_acct.key_location = por_locations.salesforce_id
left join por_regions as por_regions on por_locations.region_id = por_regions.id
left join por_continents as por_continents on por_regions.continent_id = por_continents.id
left join por_entities as por_entities on por_locations.entity_id = por_entities.id
left join all_employees as emp_rm on por_regions.ukg_regional_manager_id = emp_rm.key
left join silver_acct parent_account1 on silver_acct.key_parent_account = parent_account1.key
left join silver_acct parent_account2 on parent_account1.key_parent_account = parent_account2.key
left join silver_acct parent_account3 on parent_account2.key_parent_account = parent_account3.key
left join silver_acct parent_account4 on parent_account3.key_parent_account = parent_account4.key
left join silver_acct parent_account5 on parent_account4.key_parent_account = parent_account5.key
left join silver_acct parent_account6 on parent_account5.key_parent_account = parent_account6.key
left join silver_acct parent_account7 on parent_account6.key_parent_account = parent_account7.key
left join silver_acct parent_account8 on parent_account7.key_parent_account = parent_account8.key
left join silver_acct parent_account9 on parent_account8.key_parent_account = parent_account9.key