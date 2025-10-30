{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_rate_card_set"
    )
}}

with
    sfc_rate_card_set as (select * from {{ source("salesforce", "pse_rate_card_set_c") }} where _fivetran_deleted = false),

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
        sfc.id as key,
        md5(sfc.id) as hash_key,
        sfc.id as link,
        md5(sfc.id) as hash_link,
        sfc.owner_id as key_owner,
        md5(sfc.owner_id) as hash_key_owner,
        sfc.created_by_id as src_created_by_id,
        sfc.last_modified_by_id as src_modified_by_id,
        sfc.currency_iso_code as currency_iso_code,
        sfc.filter_by_dates_c as bln_filter_by_dates,
        sfc.active_c as bln_is_active,
        sfc.expiration_date_c as dte_expiration,
        sfc.created_date as dts_src_created,
        sfc.last_modified_date as dts_src_modified,
        sfc.system_modstamp as dts_system_modstamp,
        sfc.name as name,
        sfc.type_c as type,
    from sfc_rate_card_set sfc
) 

select * from final