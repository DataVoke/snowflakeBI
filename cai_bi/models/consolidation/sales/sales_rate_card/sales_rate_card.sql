{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_rate_card"
    )
}}

with
    sfc_rate_card as (
        select rc.*, j.pse_rate_card_set_c
        from {{ source("salesforce", "pse_rate_card_set_junction_c") }} as j
        inner join {{ source("salesforce", "pse_rate_card_c") }} as rc on j.pse_rate_card_c = rc.id
        where j._fivetran_deleted = false and rc._fivetran_deleted = false
    ),

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
            sfc_rate_card.id as key,
            md5(sfc_rate_card.id) as hash_key,
            sfc_rate_card.id as link,
            md5(sfc_rate_card.id) as hash_link,
            sfc_rate_card.owner_id as key_owner,
            md5(sfc_rate_card.owner_id) as hash_key_owner,
            sfc_rate_card.pse_rate_card_set_c as key_rate_card_set,
            md5(sfc_rate_card.pse_rate_card_set_c) as hash_key_rate_card_set,
            sfc_rate_card.pse_practice_c as key_practice,
            md5(sfc_rate_card.pse_practice_c) as hash_key_practice,
            sfc_rate_card.pse_group_c as key_group,
            md5(sfc_rate_card.pse_group_c) as hash_key_group,
            sfc_rate_card.pse_account_c as key_account,
            md5(sfc_rate_card.pse_account_c) as hash_key_account,
            sfc_rate_card.pse_region_c as key_location,
            md5(sfc_rate_card.pse_region_c) as hash_key_location,
            sfc_rate_card.created_by_id as src_created_by_id,
            sfc_rate_card.last_modified_by_id as src_modified_by_id,
            sfc_rate_card.billing_category_c as billing_category,
            sfc_rate_card.currency_iso_code as currency_iso_code,
            sfc_rate_card.pse_end_date_c as dte_end,
            sfc_rate_card.pse_start_date_c as dte_start,
            sfc_rate_card.created_date as dts_src_created,
            sfc_rate_card.last_modified_date as dts_src_modified,
            sfc_rate_card.system_modstamp as dts_system_modstamp,
            sfc_rate_card.name as name,
            sfc_rate_card.all_inclusive_rate_c as rate_all_inclusive,
            sfc_rate_card.pse_suggested_bill_rate_c as rate_suggested,
            sfc_rate_card.pse_role_c as role
        from sfc_rate_card
    ) 
    
    select * from final

