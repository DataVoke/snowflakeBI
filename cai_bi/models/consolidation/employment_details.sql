{{
    config(
        materialized='table',
    )
}}

with
    job_history as (select * from {{ source('ukg_pro', 'employee_job_history') }}),
    employment as (select * from {{ source('ukg_pro', 'employment') }}),
    company as (select * from {{ source('ukg_pro', 'company') }}),
    job as (select * from {{ source('ukg_pro', 'job') }}),

ukg as (
    select
        *
    from job
)

select * from ukg
    -- from FIVETRAN_DATABASE.UKG_PRO.EMPLOYEE_JOB_HISTORY eh left join 
    -- FIVETRAN_DATABASE.UKG_PRO.EMPLOYMENT e on eh.employee_id = e.employee_id and eh.company_id = e.company_id
    -- left join FIVETRAN_DATABASE.UKG_PRO.COMPANY c on eh.company_id = c.id
    -- left join FIVETRAN_DATABASE.UKG_PRO.JOB j on j.id = eh.job_id