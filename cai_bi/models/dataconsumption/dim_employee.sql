{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="employee"
    )
}}

with
    ukg_employee         as (select * from {{ ref('employee') }}),
    states               as (select * from {{ source('portal', 'states') }} where _fivetran_deleted = false),
    contractor_companies as (select * from {{ source('portal', 'contractor_companies') }} where _fivetran_deleted = false),
    countries            as (select * from {{ source('portal', 'countries') }} where _fivetran_deleted = false),
    departments          as (select * from {{ source('portal', 'departments') }} where _fivetran_deleted = false),
    dol_statuses         as (select * from {{ source('portal', 'dol_statuses') }} where _fivetran_deleted = false),
    employee_types       as (select * from {{ source('portal', 'employee_types') }} where _fivetran_deleted = false),
    entities             as (select * from {{ source('portal', 'entities') }} where _fivetran_deleted = false),
    ethnic_backgrounds   as (select * from {{ source('portal', 'ethnic_backgrounds') }} where _fivetran_deleted = false),
    labor_categories     as (select * from {{ source('portal', 'labor_categories') }} where _fivetran_deleted = false),
    genders              as (select * from {{ source('portal', 'genders') }} where _fivetran_deleted = false),
    locations_intacct    as (select * from {{ source('portal', 'locations_intacct') }} where _fivetran_deleted = false),
    sageint_locations as (
    select * from {{ source('sage_intacct','location') }} where _fivetran_deleted = false
),
    locations_ukg        as (select * from {{ source('portal', 'locations_ukg') }} where _fivetran_deleted = false),
    locations            as (select * from {{ source('portal', 'locations') }} where _fivetran_deleted = false),
    location_regions     as (select * from {{ source('portal', 'location_regions') }} where _fivetran_deleted = false),
    location_continents  as (select * from {{ source('portal', 'location_continents') }} where _fivetran_deleted = false),
    payroll_companies    as (select * from {{ source('portal', 'payroll_companies') }} where _fivetran_deleted = false),
    pay_types            as (select * from {{ source('portal', 'pay_types') }} where _fivetran_deleted = false),
    positions            as (select * from {{ source('portal', 'positions') }} where _fivetran_deleted = false),
    position_families    as (select * from {{ source('portal', 'position_families') }} where _fivetran_deleted = false),
    practices            as (select * from {{ source('portal', 'practices') }} where _fivetran_deleted = false),
    termination_types    as (select * from {{ source('portal', 'termination_types') }} where _fivetran_deleted = false),
    users_forecasts      as (select * from {{ source('portal', 'users_forecasts') }} where _fivetran_deleted = false),
    base_teams           as (select * from {{ source('portal', 'base_teams') }} where _fivetran_deleted = false),
    job_salary_grades    as (select * from {{ source('portal', 'job_salary_grades') }} where _fivetran_deleted = false),
    ukg_companies        as (select * from {{ source('ukg_pro', 'company') }} where _fivetran_deleted = false)
SELECT 
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    --keys
    ukg.key,
    ukg.key_base_team,
    companies.record_id as key_company,
    continents.record_id as key_continent,
    contractor_companies.record_id as key_contractor_company,
    countries.record_id as key_country,
    departments.record_id as key_department,
    dol.record_id as key_dol_status,
    employee_types.record_id as key_employee_type,
    coalesce(entities.record_id, por_entities.record_id) as key_entity,
    ethnic.record_id as key_ethnic_background_id,
    genders.record_id as key_gender,
    labor_categories.record_id as key_labor_categories,
    locations.record_id as key_location,
    cast(locations_intacct.record_id as string) as key_location_intacct,
    locations_ukg.record_id as key_location_ukg,
    payroll_companies.record_id as key_payroll_company,
    pay_types.record_id as key_pay_type,
    positions.record_id as key_position,
    position_families.record_id as key_position_family,
    practices.record_id as key_practice,
    regions.record_id as key_region,
    states.record_id as key_state,
    supervisors.key as key_supervisor,
    termination_types.record_id as key_termination_type,

    -- ids
    ukg.employee_id,
    sfc.account_id,
    por.azure_id,
    sin.intacct_contact_key,
    sin.intacct_department_key,
    sin.intacct_location_key,
    sin.intacct_employee_id,
    sin.key as intacct_employee_key,
    por.intacct_override_entity_id,
   coalesce(nullif(sageint_locations.parentid,''),nullif(sin.location_id,''),nullif(entities.display_name,''),nullif(por_entities.display_name,'')) as entity_id,
    ukg.national_id,
    ukg.national_id_country,
    por.key as portal_id,
    sfc.profile_id,
    por.region_id,
    sfc.key as salesforce_contact_id,
    por.salesforce_user_id,
    por.salesforce_sandbox_contact_id,
    por.salesforce_sandbox_user_id,
    por.tracker_record_id,
    por.ukg_override_payroll_company_id,
    ukg.ukg_person_id,
    sfc.work_calendar_id,
    ukg.key_entity as company_id,
    ukg.job_salary_grade_id,

--names
    continents.display_name as continent_name,
    companies.display_name as company_name,
    contractor_companies.display_name as contractor_company_name,
    countries.display_name as country_name,
    departments.display_name as department_name,
    dol.display_name as dol_status_name,
    employee_types.display_name as employee_type_name,
    case when entities.display_name is null or entities.display_name = '' then por_entities.display_name else entities.display_name end as entity_name,
    ethnic.display_name as ethnic_background_name,  
    genders.display_name as gender_name,
    labor_categories.display_name as labor_category_name,
    locations.display_name as location_name,
    locations_intacct.display_name as location_name_intacct,
    locations_ukg.display_name as location_name_ukg,
    payroll_companies.display_name as payroll_company_name,
    pay_types.display_name as pay_type_name,
    ifnull(positions.display_name, ukg.job_title) as position_name,
    position_families.display_name as position_family_name,
    practices.display_name as practice_name,
    regions.display_name as region_name,
    states.display_name as state_name,
    supervisors.display_name as supervisor_name,
    termination_types.display_name as termination_type_name,
    termination_types.addressable_type as termination_addressable_type,
    base_teams.display_name as base_team_name,
    job_salary_grades.display_name as job_salary_grade_name,

-- other fields
    ukg.address_city,
    ukg.address_country,
    ukg.address_postal_code,
    ukg.address_state,
    ukg.address_street,
    ukg.annual_salary,
    ukg.bln_exclude_from_resource_planner,
    ukg.bln_is_active,
    sin.bln_is_hourly,
    por.bln_mst,
    por.bln_pm_qualified,
    sfc.bln_is_resource,
    sfc.closed_won_goal,
    ukg_companies.code as company_code,
    ukg.currency_code,
    ukg.display_name,
    ukg.display_name_lf,
    ukg.dte_birth,
    sfc.dte_of_industry_experience,
    ukg.dte_src_end,
    ukg.dte_src_start,
    ukg.dts_in_job,
    ukg.dts_last_hire,
    ukg.dts_last_paid,
    ukg.email_address_personal,
    ukg.email_address_work,
    ukg.employee_status,
    ukg.first_name,
    ukg.first_name_display,
    ukg.former_name,
    ukg.historical_utilization_target_hours,
    ukg.home_phone,
    ukg.home_phone_country,
    ukg.hourly_pay_rate,
    sin.intacct_contact_name,
    job_salary_grades.salary_grade as job_salary_grade,
    ifnull(nullif(ukg.ukg_override_job_title,''),ifnull(positions.display_name, ukg.job_title)) as job_title,
    ukg.last_name,
    ukg.middle_name,
    ukg.other_rate_1,
    ukg.other_rate_2,
    ukg.other_rate_3,
    ukg.other_rate_4,
    ukg.pay_group,
    ukg.pay_period_pay_rate,
    ukg.status,
    ukg.term_type,
    ukg.termination_reason_description,
    ukg.total_ann_salary,
    ukg.ukg_employee_number,
    ukg.ukg_override_job_title,
    ukg.ukg_status,
    ukg.utilization_target,
    ukg.utilization_target_hours,
    ukg.weekly_pay_rate,
    ukg.work_phone_country,
    por.work_phone_number,
    nullif(ukg.dietary_needs,'') as dietary_needs,
    nullif(ukg.metric_bonus_type,'') as metric_bonus_type,
    nullif(ukg.vest_size,'') as vest_size,

    --target billing data
    users_forecasts.bill_rate as target_bill_rate_current,
    users_forecasts.plan_hours_week as target_bill_hours_week_current,
    users_forecasts.plan_hours_year as target_bill_hours_year_current,
    users_forecasts.plan_bill_amount_week as target_bill_amount_week_current,
    users_forecasts.plan_bill_amount_year as target_bill_amount_year_current,
    users_forecasts_last_year.bill_rate as target_bill_rate_last,
    users_forecasts_last_year.plan_hours_week as target_bill_hours_week_last,
    users_forecasts_last_year.plan_hours_year as target_bill_hours_year_last,
    users_forecasts_last_year.plan_bill_amount_week as target_bill_amount_week_last,
    users_forecasts_last_year.plan_bill_amount_year as target_bill_amount_year_last
from ukg_employee ukg 
left join ukg_employee sin on ukg.hash_link = sin.hash_link and sin.src_sys_key = 'int'
left join ukg_employee por on ukg.hash_link = por.hash_link and por.src_sys_key = 'por'
left join ukg_employee sfc on ukg.hash_link = sfc.hash_link and sfc.src_sys_key = 'sfc'
left join states as states on ukg.state_id = states.ukg_id
left join contractor_companies as contractor_companies on por.contractor_company_id = contractor_companies.id
left join countries as countries on ukg.address_country = countries.ukg_id
left join departments as departments on ukg.department_id = departments.ukg_id
left join dol_statuses as dol on ukg.dol_status_id = dol.ukg_id
left join employee_types as employee_types on ukg.employee_type_id = employee_types.ukg_id
left join entities as companies on ukg.key_entity = companies.ukg_id
left join ethnic_backgrounds as ethnic on ukg.ethnic_background_id = ethnic.ukg_id
left join sageint_locations on sin.intacct_location_key = sageint_locations.recordno
left join entities entities on ifnull(sageint_locations.parentkey,sin.intacct_location_key) = entities.id
left join entities por_entities on por.key_entity = por_entities.id
--left join entities entities on sin.location_id_intacct = entities.id
left join labor_categories as labor_categories on por.labor_category_id = labor_categories.id
left join genders as genders on ukg.gender_id = genders.ukg_id
left join locations_intacct as locations_intacct on ukg.location_id_intacct = locations_intacct.ukg_id
left join locations_ukg as locations_ukg on ukg.location_id_ukg = locations_ukg.ukg_id
left join locations as locations on ifnull(states.location_id,countries.location_id) = locations.id
left join location_regions as regions on locations.region_id = regions.id
left join location_continents as continents on regions.continent_id = continents.id
left join payroll_companies as payroll_companies on ukg.payroll_company_id = payroll_companies.ukg_id
left join pay_types as pay_types on ukg.pay_type_id = pay_types.ukg_id
left join positions as positions on ukg.position_id = positions.ukg_id
left join position_families as position_families on ukg.position_family_id = position_families.ukg_id
left join practices as practices on ukg.practice_id = practices.ukg_id
left join ukg_employee as supervisors on ukg.supervisor_id =supervisors.key
left join termination_types as termination_types on ukg.termination_type_id = termination_types.ukg_id
left join users_forecasts as users_forecasts on por.key = users_forecasts.user_id and users_forecasts.timeframe_id = year(current_date())
left join users_forecasts as users_forecasts_last_year on por.key = users_forecasts_last_year.user_id and users_forecasts_last_year.timeframe_id = year(current_date())-1
left join base_teams as base_teams on base_teams.ukg_id = ukg.key_base_team
left join job_salary_grades as job_salary_grades on ukg.job_salary_grade_id = job_salary_grades.id
left join ukg_companies as ukg_companies on ukg.key_entity = ukg_companies.id
where ukg.src_sys_key = 'ukg'