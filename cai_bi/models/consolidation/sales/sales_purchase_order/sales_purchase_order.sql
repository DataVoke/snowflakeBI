{{
    config(
        materialized="table",
        schema="consolidation",
        alias="sales_purchase_order"
    )
}}

with
    sfc_purchase_order as (select * from {{ source("salesforce", "purchase_order_c") }} where _fivetran_deleted = false),

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
        proposal_c as key_proposal,
        md5(proposal_c) as hash_key_proposal,
        created_by_id as src_created_by_id,
        last_modified_by_id as src_modified_by_id,
        amount_c as amount,
        currency_iso_code as currency_iso_code,
        date_c as dte,
        created_date as dts_src_created,
        last_modified_date as dts_src_modified,
        system_modstamp as dts_system_modstamp,
        name as name,
        notes_c as notes,
        purchase_order_number_c as purchase_order_number,
        title_c as title,
        type_c as type
    from sfc_purchase_order
) select * from final
