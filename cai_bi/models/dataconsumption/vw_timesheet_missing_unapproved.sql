{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_timesheet_missing_unapproved"
    )
}}

select  e.employee_id, e.display_name, e.display_name_lf, e.supervisor_name, e.key_supervisor,sup.email_address_work as supervisor_email,  e.region_name, e.region_id, e.key_region, e.department_name, e.key_department,
date(e.dts_last_hire),  e.DTE_SRC_START, e.DTE_SRC_END, e.UTILIZATION_TARGET_HOURS, e.TARGET_BILL_HOURS_WEEK_CURRENT, e.location_name, e.status,
'Missing' as type , null as project_id, null as project_name, null as project_manager_name, null as key_project_manager, null as email_address_work
from  {{ref('dim_employee')}} e left join 
 {{ref('fct_timesheet')}} t 
 on e.key = t.key_employee left join {{ref('dim_employee')}} sup on sup.key = e.key_supervisor where t.key is null
union all
 select e.employee_id, ge.display_name, ge.display_name_lf, ge.supervisor_name, ge.key_supervisor,sup.email_address_work as supervisor_email,   
 ge.region_name, ge.region_id, ge.key_region, ge.department_name, ge.key_department,
date(ge.dts_last_hire),  ge.DTE_SRC_START, ge.DTE_SRC_END, ge.UTILIZATION_TARGET_HOURS, ge.TARGET_BILL_HOURS_WEEK_CURRENT, ge.location_name, ge.status,'Unapproved' as type ,gold_prj.project_id, gold_prj.project_name, gold_prj.project_manager_name, gold_prj.key_project_manager, gold_prj.email_address_work
 from
{{ref('employee')}} e left join {{ref('timesheet')}} t on e.hash_key = t.hash_key_employee 
and e.src_sys_key = t.src_sys_key and t.src_sys_key ='sfc'
left join {{ref('dim_employee')}} ge on ge.employee_id = e.employee_id 
left join {{ref('dim_employee')}} sup on sup.key = ge.key_supervisor 
left join (select distinct key_timesheet, key_project from {{ref('timesheet_entry')}} where src_sys_key='sfc' ) te on 
te.key_timesheet = t.key 
left join {{ref('project')}} prj on te.key_project = prj.key  and prj.src_sys_key ='sfc'
left join {{ref('project')}} int_prj on prj.hash_link = int_prj.hash_link and int_prj.src_sys_key ='int'
left join {{ref('dim_project')}} gold_prj on int_prj.key = gold_prj.key
where e.src_sys_key ='sfc' and t.hash_key_employee is not null and t.status <> 'Approved' 