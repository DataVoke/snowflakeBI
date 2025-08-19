{{
    config(
        materialized="table",
        schema="consolidation",
        alias="opportunity_team_member"
    )
}}

with
    sfc_account as (select * from {{ source("salesforce", "opportunity_team_member") }} where _fivetran_deleted = false),

final as (
    select
        'sfc' as src_sys_key,
        cast(current_timestamp as timestamp_tz) as dts_created_at,
        '{{ this.name }}' as created_by,
        cast(current_timestamp as timestamp_tz) as dts_updated_at,
        '{{ this.name }}' as updated_by,
        cast(current_timestamp as timestamp_tz) as dts_eff_start,
        cast('9999-12-31' as timestamp_tz ) as dts_eff_end,
        true as bln_current,
        id as key,
        md5(id) as hash_key,
        id as link,
        md5(id) as hash_link,
        opportunity_id as key_opportunity,
        md5(opportunity_id) as hash_key_opportunity,
        user_id as key_user,
        md5(user_id) as hash_key_user,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        currency_iso_code as currency_iso_code,
        cast(created_date as timestamp_tz) as dts_src_created,
        cast(last_modified_date as timestamp_tz) as dts_src_modified,
        cast(system_modstamp as timestamp_tz) as dts_system_modstamp,
        name as name,
        opportunity_access_level as opportunity_access_level,
        team_member_role as team_member_role,
        title as title
    from sfc_account
) select * from final
