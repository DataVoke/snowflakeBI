{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_estimate_phase_code_products"
    )
}}

with
    estimatePhaseCodes as (
        select pc.hours, pc.budget, pc.name, pc.estimatetotalid, pc.recordid,  e.sfproposalid, c.code as currency_id, e.estimateid
        from {{ source("psatools","estimatetotals_phasecodes") }} pc
        left join {{ source("psatools","estimatetotals") }} et on pc.estimatetotalid = et.estimatetotalid
        left join {{ source("psatools","estimate") }} e on et.estimateid = e.estimateid
        left join {{ source("psatools","currency") }} c on e.currencyid = c.currencyid
        where pc._fivetran_deleted=false and et._fivetran_deleted=false and e._fivetran_deleted=false
    ),
    proposals as (
        select key, name, quote_number, key_opportunity, key_project, project_id, project_name, dte_internal_approval, status
        from {{ ref('dim_sales_proposal') }}
    ),
    opportunities as (
        select key, key_location, location_name, stage_name, key_practice, practice_name, name, contract_type, dte_proposal_submitted, dte_close, key_account
        from {{ ref('dim_sales_opportunity') }}
    ),
    accounts as (
        select key, name, key_top_level_parent_account, top_level_parent_account_name
        from {{ ref('dim_sales_account') }}
    ),
    pclinks as (
        select spc.id as standard_phase_code_id, spcl.phase_code, spc.product_id 
        from {{ source("psatools","standard_phase_codes_product_links") }} spcl
        left join {{ source("psatools","standard_phase_codes") }} spc on spcl.standard_phase_code_id = spc.id
        where spc.bln_is_billable = true
    ),
    currency_conversion as (
        select 
            frm_curr, 
            to_curr, 
            date, 
            fx_rate_mul
        from {{ ref("ref_fx_rates_timeseries") }}  as cc
        where frm_curr in (select currency from {{ ref('currencies_active') }})
        and to_curr in (select currency from {{ ref('currencies_active') }})
    )
select 
    pc.recordid as key,
    pc.estimatetotalid as key_estimate_total,
    pc.estimateid as key_estimate,
    p.key_opportunity,
    p.key as key_proposal,
    a.key as key_account,
    o.key_location,
    o.key_practice,
    p.key_project,
    a.key_top_level_parent_account,
    a.top_level_parent_account_name as client_name,
    a.name as client_site_name,
    o.name as opportunity_name,
    p.project_id,
    p.project_name,
    o.practice_name,
    o.location_name,
    p.name as proposal_name,
    p.quote_number as proposal_number,
    o.contract_type,
    pc.name as phase_code,
    ifnull(pcl.standard_phase_code_id,'OTHER') as standard_phase_code,
    ifnull(pcl.product_id, 'Regular Labor Sales') as product, 
    pc.hours,
    cast(pc.budget as number(38,2)) as cost_original,
    cast(cost_original * ifnull(cc_to_usd.fx_rate_mul,1) as number(38,2)) as cost_usd,
    pc.currency_id as currency_iso_code,
    p.dte_internal_approval, 
    o.dte_proposal_submitted, 
    o.dte_close,  
    coalesce(p.dte_internal_approval, o.dte_proposal_submitted, o.dte_close) as dte_currency_conversion,
    o.stage_name as opportunity_status, 
    p.status as proposal_status,
from estimatePhaseCodes pc
left join proposals p on pc.sfproposalid = p.key
left join opportunities o on p.key_opportunity = o.key
left join accounts a on o.key_account = a.key
left join pclinks pcl on pc.name = pcl.phase_code
left join currency_conversion as cc_to_usd on (
                        cc_to_usd.frm_curr = pc.currency_id
                        and cc_to_usd.to_curr = 'USD'
                        and cc_to_usd.date = (case
                                                when coalesce(p.dte_internal_approval, o.dte_proposal_submitted, o.dte_close) < '2016-01-02' then '2016-01-04' -- earliest date we have is 1/4/2016
                                                else coalesce(p.dte_internal_approval, o.dte_proposal_submitted, o.dte_close)
                                            end)
                    )
where key_proposal is not null