{{
    config(
        materialized="table",
        schema="consolidation",
        alias="account_team_member"
    )
}}

with
    sfc_account as (select * from {{ source("salesforce", "account_team_member") }} where _fivetran_deleted = false),

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
        account_id as key_account,
        md5(account_id) as hash_key_account,
        user_id as key_user,
        md5(user_id) as hash_key_user,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        account_access_level as account_access_level,
        case_access_level as case_access_level,
        contact_access_level as contact_access_level,
        currency_iso_code as currency_iso_code,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        opportunity_access_level as opportunity_access_level,
        team_member_role as team_member_role,
        title as title
    from sfc_account
) select * from final
