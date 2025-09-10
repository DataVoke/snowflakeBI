{{ config(
    materialized = 'table',
    schema = 'dataconsumption',
    alias="reporting_groups_locations"
) }}

with practice_3 as (
    select
concat(l.id,'|3') as id,
            r.id as group_id, 
            r.parent_id as parent_group_id,
            l.id as location_id, 
            r.display_name as group_name, 
            r.start_date as start_date,
            r.end_date as end_date,
            l.record_id as key_location, 
            loc.id as por_location_id, 
            loc.salesforce_id as sfc_location_id, 
            loc.intacct_id as int_location_id, 
            cast(p.record_id as varchar(255)) as key_practice, 
            cast(p.id as varchar(255)) as por_practice_id, 
            cast(p.salesforce_id as varchar(255)) as sfc_practice_id, 
            cast(p.intacct_id as varchar(255)) as int_practice_id,
            iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
            r.sort
        from {{ source('portal', 'reporting_groups_locations') }} l
        left join {{ source('portal', 'reporting_groups') }} r on l.group_id=r.id
        left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
        left join {{ source('portal', 'practices') }} p on p.id = 3
        where practice_id like '%|3|%' and r.visible = true
),

practice_2 as (
            select 
                concat(l.id,'|2') as id,
                r.id as group_id, 
                r.parent_id as parent_group_id,
                l.id as location_id, 
                r.display_name as group_name, 
                r.start_date as start_date,
                r.end_date as end_date,
                l.record_id as key_location, 
                loc.id as por_location_id, 
                loc.salesforce_id as sfc_location_id, 
                loc.intacct_id as int_location_id, 
                cast(p.record_id as varchar(255)) as key_practice, 
                cast(p.id as varchar(255)) as por_practice_id, 
                cast(p.salesforce_id as varchar(255)) as sfc_practice_id, 
                cast(p.intacct_id as varchar(255)) as int_practice_id,
                iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
                r.sort
        from {{ source('portal', 'reporting_groups_locations') }} l
        left join {{ source('portal', 'reporting_groups') }} r on l.group_id=r.id
        left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
        left join {{ source('portal', 'practices') }} p on p.id = 2 
            where practice_id like '%|2|%' and r.visible = true
),

practice_4 as (
            select 
                concat(l.id,'|4') as id,
                r.id as group_id, 
                r.parent_id as parent_group_id,
                l.id as location_id, 
                r.display_name as group_name, 
                r.start_date as start_date,
                r.end_date as end_date,
                l.record_id as key_location, 
                loc.id as por_location_id, 
                loc.salesforce_id as sfc_location_id, 
                loc.intacct_id as int_location_id, 
                cast(p.record_id as varchar(255)) as key_practice, 
                cast(p.id as varchar(255)) as por_practice_id, 
                cast(p.salesforce_id as varchar(255)) as sfc_practice_id, 
                cast(p.intacct_id as varchar(255)) as int_practice_id,
                iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
                r.sort
        from {{ source('portal', 'reporting_groups_locations') }} l
        left join {{ source('portal', 'reporting_groups') }} r on l.group_id=r.id
        left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
        left join {{ source('portal', 'practices') }} p on p.id = 4 
            where practice_id like '%|4|%' and r.visible = true
)
,

practice_5 as (
            select 
                concat(l.id,'|5') as id,
                r.id as group_id, 
                r.parent_id as parent_group_id,
                l.id as location_id, 
                r.display_name as group_name, 
                r.start_date as start_date,
                r.end_date as end_date,
                l.record_id as key_location, 
                loc.id as por_location_id, 
                loc.salesforce_id as sfc_location_id, 
                loc.intacct_id as int_location_id, 
                cast(p.record_id as varchar(255)) as key_practice, 
                cast(p.id as varchar(255)) as por_practice_id, 
                cast(p.salesforce_id as varchar(255)) as sfc_practice_id, 
                cast(p.intacct_id as varchar(255)) as int_practice_id,
                iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
                r.sort
        from {{ source('portal', 'reporting_groups_locations') }} l
        left join {{ source('portal', 'reporting_groups') }} r on l.group_id=r.id
        left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
        left join {{ source('portal', 'practices') }} p on p.id = 5
            where practice_id like '%|5|%' and r.visible = true
),
practice_internal as (
            select 
                concat(l.id,'|') as id,
                r.id as group_id, 
                r.parent_id as parent_group_id,
                l.id as location_id, 
                r.display_name as group_name, 
                r.start_date as start_date,
                r.end_date as end_date,
                l.record_id as key_location, 
                loc.id as por_location_id, 
                loc.salesforce_id as sfc_location_id, 
                loc.intacct_id as int_location_id, 
                '' as key_practice, 
                '' as por_practice_id, 
                '' as sfc_practice_id, 
                '' as int_practice_id,
                iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
                r.sort
        from {{ source('portal', 'reporting_groups_locations') }} l
        left join {{ source('portal', 'reporting_groups') }} r on l.group_id=r.id
        left join {{ source('portal', 'locations') }} loc on l.location_id = loc.id
            select 
                concat(l.id,'|') as id,
                r.id as group_id, 
                r.parent_id as parent_group_id,
                l.id as location_id, 
                r.display_name as group_name, 
                r.start_date as start_date,
                r.end_date as end_date,
                l.record_id as key_location, 
                loc.id as por_location_id, 
                loc.salesforce_id as sfc_location_id, 
                loc.intacct_id as int_location_id, 
                '' as key_practice, 
                '' as por_practice_id, 
                '' as sfc_practice_id, 
                '' as int_practice_id,
                iff(current_date>=r.start_date and current_date<r.end_date,true, false) as active,
                r.sort
        from prod_bi_dw.cai_portal.reporting_groups_locations l
        left join prod_bi_dw.cai_portal.reporting_groups r on l.group_id=r.id
        left join prod_bi_dw.cai_portal.locations loc on l.location_id = loc.id
            where r.parent_id = 'I004' or r.id= 'I004' and r.visible = true
)

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_3.*
from practice_3

union all

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_2.*
from practice_2

union all

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_4.*
from practice_4

union all

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_5.*
from practice_5

union all

select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    practice_internal.*
from practice_internal