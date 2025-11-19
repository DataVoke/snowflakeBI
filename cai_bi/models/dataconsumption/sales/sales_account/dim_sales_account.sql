{{ config(
    materialized = "table",
    schema = "dataconsumption",
    alias="sales_account"
) }}

with 
    silver_acct as (
        select *,trim(replace(replace(nullif(billing_postal_code,''),' ',''),'-','')) as postal_code_clean,
            case 
                when upper(billing_country_code) = 'US' then trim(left(postal_code_clean,5) )
                when upper(billing_country_code) = 'CA' then trim(left(postal_code_clean,3))
                when upper(billing_country_code) = 'GB' then trim(left(postal_code_clean, length(postal_code_clean) - 3))
                when upper(billing_country_code) = 'IE' then trim(left(postal_code_clean,3))
                when upper(billing_country_code) = 'NL' then trim(left(postal_code_clean,4))
                when upper(billing_country_code) = 'KR' then trim(left(postal_code_clean,5))
                else postal_code_clean
            end as geo_postal_code
        from {{ ref('sales_account') }} where src_sys_key = 'sfc' 
    ),
    ukg_employees as (
        select ifnull(sfc.salesforce_user_id, por.salesforce_user_id) as sfc_user_id, por.salesforce_user_id, ifnull(sfc.key, por.contact_id) as sfc_contact_id, ukg.* 
        from {{ ref('employee') }} ukg
        left join {{ ref('employee') }} as sfc on ukg.link = sfc.link and sfc.src_sys_key = 'sfc'
        left join {{ ref('employee') }} as por on ukg.link = por.link and por.src_sys_key = 'por'
        where ukg.src_sys_key = 'ukg'
    ),
    silver_rcs as (
        select * from {{ ref('sales_rate_card_set') }} where src_sys_key = 'sfc'
    ),
    por_practices as (
        select * from {{ source('portal','practices') }} where _fivetran_deleted = false
    ),
    por_locations as (
        select * from {{ source('portal','locations') }} where _fivetran_deleted = false
    ),
    por_regions as (
        select * from {{ source('portal','location_regions') }} where _fivetran_deleted = false
    ),
    por_states as (
        select * from {{ source('portal','states') }} where _fivetran_deleted = false
    ),
    por_countries as (
        select * from {{ source('portal','countries') }} where _fivetran_deleted = false
    ),
    geo_locations as (select *, upper(trim(replace(replace(nullif(postal_code,''),' ',''),'-',''))) as postal_code_clean from {{ source('reference', 'postal_code_geolocations') }})

select 
    silver_acct.key,
    acct_client_manager.key as key_client_manager,
    key_acct_manager.key as key_account_manager,
    por_locations.record_id as key_location,
    por_regions.record_id as key_region,
    por_countries.record_id as key_country,
    por_states.record_id as key_state,
    acct_owner.key as key_owner,
    silver_acct.key_parent_account as key_parent_account,
    por_practices.record_id as key_practice,
    acct_coord.key as key_account_coordinator,
    silver_acct.key_rate_card_set,
    coalesce(parent_account8.key,parent_account7.key,parent_account6.key,parent_account5.key,parent_account4.key,parent_account3.key,parent_account2.key,parent_account1.key) as key_top_level_parent_account,
    silver_acct.external_id,
    silver_acct.intacct_customer_id,
    silver_acct.intacct_id,
    silver_acct.src_created_by_id,
    silver_acct.src_modified_by_id,
    silver_acct.account_source,
    silver_acct.annual_revenue,
    silver_acct.billing_city,
    silver_acct.billing_country,
    silver_acct.billing_country_code,
    silver_acct.billing_postal_code,
    silver_acct.billing_state,
    silver_acct.billing_state_code,
    silver_acct.billing_street,
    silver_acct.bln_is_confidential_account,
    silver_acct.bln_is_key_account,
    silver_acct.bln_msa_in_place,
    silver_acct.currency_iso_code,
    silver_acct.description,
    silver_acct.dte_last_activity,
    silver_acct.dts_src_created,
    silver_acct.dts_src_modified,
    silver_acct.dts_system_modstamp,
    silver_acct.fax,
    coalesce(silver_acct.grade, parent_account1.grade, parent_account2.grade, parent_account3.grade, parent_account4.grade, parent_account5.grade, parent_account6.grade, parent_account7.grade, parent_account8.grade) as grade,
    silver_acct.industry,
    silver_acct.kare_designation,
    silver_acct.name,
    silver_acct.number_of_employees,
    coalesce(silver_acct.payment_terms, parent_account1.payment_terms, parent_account2.payment_terms, parent_account3.payment_terms, parent_account4.payment_terms, parent_account5.payment_terms, parent_account6.payment_terms, parent_account7.payment_terms, parent_account8.payment_terms) as payment_terms,
    silver_acct.phone,
    silver_acct.rating,
    silver_acct.type,
    silver_acct.website,
    parent_account1.name as parent_account_name,
    nullif(ltrim(concat(ifnull(parent_account8.key,''),'|', ifnull(parent_account7.key,''), ifnull(parent_account6.key,''),'|', ifnull(parent_account5.key,''),'|', ifnull(parent_account4.key,''),'|', ifnull(parent_account3.key,''),'|', ifnull(parent_account2.key,''),'|', ifnull(parent_account1.key,'')),'|'),'') as hierarchy_path,
    coalesce(parent_account8.name, parent_account7.name, parent_account6.name, parent_account5.name, parent_account4.name, parent_account3.name, parent_account2.name, parent_account1.name) as top_level_parent_account_name,    por_countries.display_name as country_name,
    por_locations.display_name as location_name,
    por_regions.display_name as region_name,
    por_states.display_name as state_name,
    por_practices.display_name as practice_name,
    initcap(acct_client_manager.display_name) as client_manager_name,
    initcap(acct_client_manager.display_name_lf) as client_manager_name_lf,
    acct_client_manager.email_address_work as client_manager_email,
    initcap(key_acct_manager.display_name) as account_manager_name,
    initcap(key_acct_manager.display_name_lf) as account_manager_name_lf,
    key_acct_manager.email_address_work as account_manager_email,
    initcap(acct_owner.display_name) as owner_name,
    initcap(acct_owner.display_name_lf) as owner_name_lf,
    acct_owner.email_address_work as owner_email,
    initcap(acct_coord.display_name) as account_coordinator_name,
    initcap(acct_coord.display_name_lf) as account_coordinator_name_lf,
    acct_coord.email_address_work as account_coordinator_email,
    silver_rcs.name as rate_card_set_name,
    silver_acct.geo_postal_code,
    gl.latitude as geo_latitude,
    gl.longitude as geo_longitude
from silver_acct
left join ukg_employees as acct_client_manager on silver_acct.key_client_manager = acct_client_manager.sfc_contact_id
left join ukg_employees as acct_owner on silver_acct.key_owner = acct_owner.sfc_user_id
left join ukg_employees as key_acct_manager on silver_acct.key_account_manager = key_acct_manager.sfc_user_id
left join ukg_employees as acct_coord on silver_acct.key_account_coordinator = acct_coord.sfc_user_id
left join silver_rcs on silver_acct.key_rate_card_set = silver_rcs.key
left join por_practices on silver_acct.key_practice = por_practices.salesforce_id
left join por_countries on silver_acct.billing_country_code = por_countries.salesforce_id
left join por_states on silver_acct.billing_state_code = por_states.salesforce_id and por_states.country_id = por_countries.id
left join por_locations on silver_acct.key_location = por_locations.salesforce_id
left join por_regions on por_locations.region_id = por_regions.id
left join silver_acct parent_account1 on silver_acct.key_parent_account = parent_account1.key
left join silver_acct parent_account2 on parent_account1.key_parent_account = parent_account2.key
left join silver_acct parent_account3 on parent_account2.key_parent_account = parent_account3.key
left join silver_acct parent_account4 on parent_account3.key_parent_account = parent_account4.key
left join silver_acct parent_account5 on parent_account4.key_parent_account = parent_account5.key
left join silver_acct parent_account6 on parent_account5.key_parent_account = parent_account6.key
left join silver_acct parent_account7 on parent_account6.key_parent_account = parent_account7.key
left join silver_acct parent_account8 on parent_account7.key_parent_account = parent_account8.key
left join silver_acct parent_account9 on parent_account8.key_parent_account = parent_account9.key
left join geo_locations gl on silver_acct.geo_postal_code = gl.postal_code_clean and por_countries.record_id = gl.key_country