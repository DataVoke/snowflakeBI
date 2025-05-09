{{ config(
    materialized='table',
) }}

with salesforce as (
    select
        md5(t.id) RECORD_KEY,
        'SFC' SRC_SYS_KEY,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ CREATED_AT_DTS,
        '{{ this.name }}' CREATED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ UPDATED_AT_DTS,
        '{{ this.name }}' UPDATED_BY,
        t.id as RECORD_NO,
        c.PSE_API_RESOURCE_CORRELATION_ID_C EMPLOYEE_ID,
        t.PSE_START_DATE_C BEGIN_DTE,
        t.PSE_END_DATE_C END_DTE,
        t.PSE_ASSIGNMENT_C ASSIGNMENT_C
    from {{ source('salesforce', 'pse_timecard_header_c')}} t
    left join {{ source('salesforce', 'contact') }} c on c.pse_is_resource_c
),
sage_intacct as (
    select
        md5(recordno) RECORD_KEY,
        'SIN' SRC_SYS_KEY,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ CREATED_AT_DTS,
        '{{ this.model }}' CREATED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ UPDATED_AT_DTS,
        '{{ this.name }}' UPDATED_BY,
        recordno RECORD_NO,
        EMPLOYEEID EMPLOYEE_ID,
        begindate BEGIN_DTE,
        enddate END_DTE,
        state STATE,
        state_worked STATE_WORKED,
        actualcost ACTUAL_COST,
        glpostdate GLPOST_DTE
    from {{ source('sage_intacct', 'timesheet')}}
),
final as (
    select 
        RECORD_KEY,
        SRC_SYS_KEY,
        CREATED_AT_DTS,
        CREATED_BY,
        UPDATED_AT_DTS,
        UPDATED_BY,
        RECORD_NO,
        EMPLOYEE_ID,
        BEGIN_DTE,
        END_DTE,
        NULL STATE,
        NULL STATE_WORKED,
        NULL ACTUAL_COST,
        NULL GLPOST_DTE,
        ASSIGNMENT_C
    from salesforce
    union all
    select
        RECORD_KEY,
        SRC_SYS_KEY,
        CREATED_AT_DTS,
        CREATED_BY,
        UPDATED_AT_DTS,
        UPDATED_BY,
        RECORD_NO,
        EMPLOYEE_ID,
        BEGIN_DTE,
        END_DTE,
        STATE,
        STATE_WORKED,
        ACTUAL_COST,
        GLPOST_DTE,
        NULL ASSIGNMENT_C
    from sage_intacct
)

select * from final