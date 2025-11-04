{{
    config(
        materialized="table",
        schema="dataconsumption",
        alias="project_task_milestone"
    )
}}

with 
    milestones as (select * from {{ ref('project_task_milestone') }}),
    projects as (select * from {{ ref('project') }}),
    phase_codes as (select * from {{ ref('project_phase_code') }}),
    tasks as (select * from {{ ref('project_task') }} where src_sys_key='sfc'),
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
    all_employees as (
        select * from ukg_employees 
        union
        select * from sfc_employees
    ),
    sf_users as (
        select * from {{ ref('sales_user') }} where src_sys_key = 'sfc' 
    )
select 
    cast(current_timestamp as timestamp_tz) as dts_created_at,
    '{{ this.name }}' as created_by,
    cast(current_timestamp as timestamp_tz) as dts_updated_at,
    '{{ this.name }}' as updated_by,
    milestones.key,
    int_project.key as key_project,
    phase_codes.link as key_phase_code,
    milestones.key_milestone,
    milestones.key_project_task,
    created_by.key as key_created_by,
    milestones.src_created_by_id,
    milestones.src_modified_by_id,
    milestones.key_project as sfc_project_id,
    milestones.currency_iso_code,
    milestones.dte_milestone,
    milestones.dts_src_created,
    milestones.dts_src_modified,
    milestones.dts_system_modstamp,
    ifnull(milestones.earned_value,0) as earned_value,
    milestones.name,
    ifnull(milestones.perc_complete,0) as perc_complete,
    milestones.status,
    ifnull(milestones.status_indirect_tvl,0) as status_indirect_tvl,
    milestones.type,
    int_project.project_name,
    int_project.project_id,
    tasks.name as task_name,
    phase_codes.name as phase_code_name,
    ifnull(created_by.display_name, initcap(trim(concat(ifnull(sf_users_created.first_name,''),' ',ifnull(sf_users_created.last_name,''))))) as created_by_name,
    ifnull(created_by.display_name_lf, initcap(trim(concat(ifnull(sf_users_created.last_name,''),', ',ifnull(sf_users_created.first_name,''))))) as created_by_name_lf,
    ifnull(created_by.email_address_work, sf_users_created.email) as created_by_email,
    ifnull(modified_by.display_name, initcap(trim(concat(ifnull(sf_users_modified.first_name,''),' ',ifnull(sf_users_modified.last_name,''))))) as modified_by_name,
    ifnull(modified_by.display_name_lf, initcap(trim(concat(ifnull(sf_users_modified.last_name,''),', ',ifnull(sf_users_modified.first_name,''))))) as modified_by_name_lf,
    ifnull(modified_by.email_address_work, sf_users_modified.email) as modified_by_email
    
from milestones
left join projects as sf_project on milestones.hash_key_project = sf_project.hash_key and sf_project.src_sys_key = 'sfc'
left join projects as int_project on sf_project.hash_link = int_project.hash_link and int_project.src_sys_key = 'int'
left join phase_codes on milestones.hash_key_milestone = phase_codes.hash_key and phase_codes.src_sys_key = 'sfc_milestones'
left join tasks on milestones.hash_key_project_task = tasks.hash_key and tasks.src_sys_key = 'sfc'
left join all_employees as created_by on milestones.src_created_by_id = created_by.sfc_user_id
left join sf_users as sf_users_created on milestones.src_created_by_id = sf_users_created.key
left join all_employees as modified_by on milestones.src_modified_by_id = modified_by.sfc_user_id
left join sf_users as sf_users_modified on milestones.src_modified_by_id = sf_users_modified.key
where milestones.src_sys_key='sfc'

