{{ config(
    materialized='table',
) }}

with 
    pse    as (select * from {{ source('salesforce', 'pse_timecard_header_c') }} where _fivetran_deleted = false),
    sf_contact as (select * from {{ source('salesforce', 'contact') }} where _fivetran_deleted = false),
    si_time    as (select * from {{ source('sage_intacct', 'timesheet') }} where _fivetran_deleted = false),

sage_intacct as (
    select
        'int' as src_sys_key,
        current_timestamp as dts_created_at,
        'timesheet' as created_by,
        current_timestamp as dts_updated_at,
        'timesheet' as updated_by,
        current_timestamp as dts_eff_start,
        '9999-12-31' as dts_eff_end,
        true as bln_current,

        recordno as key,
        md5(recordno) as hash_key,
        concat(employeeid, begindate) as link,
        md5(concat(employeeid, begindate)) as hash_link,
        cast(employeekey as string) as key_employee,
        md5(employeekey) as hash_key_employee,
        cast(megaentitykey as string) as key_entity,
        md5(megaentitykey) as hash_key_entity,

        null as assignment_id,
        employee_departmentid as employee_department_id,
        employeeid as employee_id_intacct,
        employeeid as employee_id,
        employee_locationid as employee_location_id,

        locationkey as location_key,
        megaentityid as entity_id,
        cast(createdby as string) as src_created_by_id,
        cast(modifiedby as string) as src_modified_by_id,

        supdocid as sup_doc_id,
        supdockey as sup_doc_key,
        recordno as system_id,

        actualcost as bln_cost_actual,
        config as config,
        description as description,
        glpostdate as dte_gl_post,

        enddate as dte_src_end,
        begindate as dte_src_start,
        whencreated as dts_src_created,
        whenmodified as dts_src_modified,

        employeefirstname as employee_first_name,
        employeelastname as employee_last_name,
        employeename as employee_name,

        hoursinday as hours_in_day,
        megaentityname as mega_entity_name,
        method as method,
        record_url as record_url,

        state_worked as state_worked,
        state as status,
        uom as uom
    from si_time
),

salesforce as (
    select
        'sfc' as src_sys_key,
        current_timestamp as dts_created_at,
        '{{ this.name }}' as created_by,
        current_timestamp as dts_updated_at,
        '{{ this.name }}' as updated_by,
        current_timestamp as dts_eff_start,
        '9999-12-31' as dts_eff_end,
        true as bln_current,

        pse.id as key,
        md5(pse.id) as hash_key,
        concat(c.pse_api_resource_correlation_id_c, pse.pse_start_date_c) as link,
        md5(concat(c.pse_api_resource_correlation_id_c, pse.pse_start_date_c)) as hash_link,
        pse.pse_resource_c as key_employee,
        md5(pse.pse_resource_c) as hash_key_employee,
        c.pse_group_c as key_entity,
        md5(c.pse_group_c) as hash_key_entity,

        pse.pse_assignment_c as assignment_id,
        c.department as employee_department_id,
        c.pse_api_resource_correlation_id_c as employee_id_intacct,
        pse.pse_resource_c as employee_id,
        null as employee_location_id,

        null as location_key,
        null as entity_id,
        pse.created_by_id as src_created_by_id,
        pse.last_modified_by_id as src_modified_by_id,

        null as sup_doc_id,
        null as sup_doc_key,
        pse.id as system_id,

        null as bln_cost_actual,
        null as config,
        null as description,
        null as dte_gl_post,

        pse.pse_end_date_c as dte_src_end,
        pse.pse_start_date_c as dte_src_start,
        pse.created_date as dts_src_created,
        pse.last_modified_date as dts_src_modified,

        c.first_name as employee_first_name,
        c.last_name as employee_last_name,
        c.name as employee_name,

        null as hours_in_day,
        null as mega_entity_name,
        null as method,
        null as record_url,

        pse.pse_primary_location_c as state_worked,
        pse.pse_status_c as status,
        null as uom
    from pse
    left join sf_contact c
        on pse.pse_resource_c = c.id
),
final as (
    select * from sage_intacct
    union all
    select * from salesforce
)

select
    src_sys_key,
    cast(dts_created_at as timestamp_tz) as dts_created_at,
    created_by,
    cast(dts_updated_at as timestamp_tz) as dts_updated_at,
    updated_by,
    cast(dts_eff_start as timestamp_tz) as dts_eff_start,
    cast(dts_eff_end as timestamp_tz) as dts_eff_end,
    bln_current,
    key,
    hash_key,
    link,
    hash_link,
    key_employee,
    hash_key_employee,
    key_entity,
    hash_key_entity,
    assignment_id,
    employee_department_id,
    employee_id_intacct,
    employee_id,
    employee_location_id,
    location_key,
    entity_id,
    src_created_by_id,
    src_modified_by_id,
    sup_doc_id,
    sup_doc_key,
    system_id,
    bln_cost_actual,
    config,
    description,
    dte_gl_post,
    dte_src_end,
    dte_src_start,
    dts_src_created,
    dts_src_modified,
    employee_first_name,
    employee_last_name,
    employee_name,
    hours_in_day,
    mega_entity_name,
    method,
    record_url,
    state_worked,
    status,
    uom
from final

