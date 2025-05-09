{{ config(
    materialized='table',
) }}

with salesforce as (
    select
        md5(t.id) as record_key,
        'SFC' as src_sys_key,
        current_timestamp()::TIMESTAMP_TZ as created_at_dts,
        '{{ this.name }}' as created_by,
        current_timestamp()::TIMESTAMP_TZ as updated_at_dts,
        '{{ this.name }}' as updated_by,
        t.id as record_no,
        c.pse_api_resource_correlation_id_c as employee_id,
        t.pse_start_date_c as begin_dte,
        t.pse_end_date_c as end_dte,
        t.pse_assignment_c as assignment_c
    from {{ source('salesforce', 'pse_timecard_header_c') }} as t
    left join {{ source('salesforce', 'contact') }} as c on c.pse_is_resource_c
),

sage_intacct as (
    select
        md5(recordno) as record_key,
        'SIN' as src_sys_key,
        current_timestamp()::TIMESTAMP_TZ as created_at_dts,
        '{{ this.model }}' as created_by,
        current_timestamp()::TIMESTAMP_TZ as updated_at_dts,
        '{{ this.name }}' as updated_by,
        recordno as record_no,
        employeeid as employee_id,
        begindate as begin_dte,
        enddate as end_dte,
        state,
        state_worked,
        actualcost as actual_cost,
        glpostdate as glpost_dte
    from {{ source('sage_intacct', 'timesheet') }}
),

final as (
    select
        record_key,
        src_sys_key,
        created_at_dts,
        created_by,
        updated_at_dts,
        updated_by,
        record_no,
        employee_id,
        begin_dte,
        end_dte,
        NULL as state,
        NULL as state_worked,
        NULL as actual_cost,
        NULL as glpost_dte,
        assignment_c
    from salesforce
    union all
    select
        record_key,
        src_sys_key,
        created_at_dts,
        created_by,
        updated_at_dts,
        updated_by,
        record_no,
        employee_id,
        begin_dte,
        end_dte,
        state,
        state_worked,
        actual_cost,
        glpost_dte,
        NULL as assignment_c
    from sage_intacct
)

select * from final
