{{ config(
    materialized='table',
) }}

with salesforce as (
    select
        t.PSE_START_DATE_C,
        c.PSE_API_RESOURCE_CORRELATION_ID_C,
        t.PSE_END_DATE_C
    from {{ source('salesforce', 'pse_timecard_header_c')}} t
    left join {{ source('salesforce', 'contact') }} c on c.pse_is_resource_c
),
sage_intacct as (
    select
        begindate,
        employeeid,
        enddate,
        state,
        state_worked,
        actualcost,
        glpostdate,
        recordno
    from {{ source('sage_intacct', 'timesheet')}}
),
final as (
    select
        'si' as src_sys_key,
        begindate,
        employeeid,
        enddate,
        state,
        state_worked,
        actualcost,
        glpostdate,
        recordno
    from sage_intacct
    union all
    select 
        'sf' as src_sys_key,
        PSE_START_DATE_C as begindate,
        PSE_API_RESOURCE_CORRELATION_ID_C as employeeid,
        PSE_END_DATE_C as enddate,
        null as state,
        null as state_worked,
        null as actualcost,
        null as glpostdate,
        null as recordno
    from salesforce
)
select 
    *
from final