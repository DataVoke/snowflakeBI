{{ config(
    materialized = 'table',
    schema = 'dataconsumption'
) }}

with practice_3 as (
    select
        concat(l.id, '|3') as id,
        r.id as group_id,
        l.id as location_id,
        r.display_name as group_name,
        l.record_id as key_location,
        loc.id as por_location_id,
        loc.salesforce_id as sfc_location_id,
        loc.intacct_id as int_location_id,
        p.record_id as key_practice,
        p.id as por_practice_id,
        p.salesforce_id as sfc_practice_id,
        p.intacct_id as int_practice_id
    from {{ source('portal', 'reporting_groups_locations') }} l
    left join {{ source('portal', 'reporting_groups') }} r on l.group_id = r.id
    left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
    left join {{ source('portal', 'practices') }} p on p.id = 3
    where r.practice_id like '%|3|%'
),

practice_2 as (
    select
        concat(l.id, '|2') as id,
        r.id as group_id,
        l.id as location_id,
        r.display_name as group_name,
        l.record_id as key_location,
        loc.id as por_location_id,
        loc.salesforce_id as sfc_location_id,
        loc.intacct_id as int_location_id,
        p.record_id as key_practice,
        p.id as por_practice_id,
        p.salesforce_id as sfc_practice_id,
        p.intacct_id as int_practice_id
    from {{ source('portal', 'reporting_groups_locations') }} l
    left join {{ source('portal', 'reporting_groups') }} r on l.group_id = r.id
    left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
    left join {{ source('portal', 'practices') }} p on p.id = 2
    where r.practice_id like '%|2|%'
)

select
    current_timestamp() as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp() as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_3.*
from practice_3

union all

select
    current_timestamp() as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp() as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_2.*
from practice_2