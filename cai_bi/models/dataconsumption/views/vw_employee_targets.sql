{{
    config(
        materialized="view",
        schema="dataconsumption",
        alias="vw_employee_targets"
    )
}}

with 
    timesheet_aggregates as (select * from {{ref('dim_timesheet_entry_aggregates')}} where entity_grouping = 'Employee'),
    ytd as (
        select dts_updated_at, 
            ukg_employee_number, 
            round(planned_hours_billable,1) as billable_hours_per_week, 
            round(52*planned_hours_billable,1) as 
            billable_hours_per_year, 
            count(date_group_id) as week_num , 
            round(sum(hours)/week_num, 1) as ytd_average_per_week, 
            round(sum(hours),1) as ytd_actual,
            round(billable_hours_per_week*week_num,1) as ytd_target, 
            round(ytd_actual/decode(ytd_target,0,1,ytd_target) * 100,1) as ytd_percentage 
        from timesheet_aggregates 
        where type ='Billable' and date_group_type_id ='W' and year =year(current_date) group by all
    ),
    last_12_mths as (
        select dts_updated_at,
            ukg_employee_number, 
            count(date_group_id) as week_num,   
            round(sum(planned_hours_billable)/week_num,1) as billable_hours_per_week, 
            round(52*billable_hours_per_week,1) as billable_hours_per_year,  
            round(sum(hours)/week_num, 1) as l12m_average_per_week, 
            round(sum(hours),1) as l12m_actual,
            round(sum(planned_hours_billable),1) as l12m_target, 
            round(l12m_actual/decode(l12m_target,0,1,l12m_target) * 100,1) as l12m_percentage 
        from timesheet_aggregates
        where type ='Billable' and date_group_type_id ='W' and to_date(date_group_display_name_1)>=dateadd(month,-12,current_date) 
        group by all
    )
select ytd.ukg_employee_number,
cast(ytd.billable_hours_per_week as number(38,2)) as billable_hours_per_week, 
cast(ytd.billable_hours_per_year as number(38,2)) as billable_hours_per_year, 
cast(ytd_average_per_week as number(38,2)) as ytd_average_per_week, 
cast(ytd_actual as number(38,2)) as ytd_actual, 
cast(ytd_target as number(38,2)) as ytd_target,
cast(ytd_percentage as number(38,2)) as ytd_percentage, 
cast(l12m_average_per_week as number(38,2)) as l12m_average_per_week, 
cast(l12m_actual as number(38,2)) as l12m_actual, 
cast(l12m_target as number(38,2)) as l12m_target, 
cast(l12m_percentage as number(38,2)) as l12m_percentage,
case when ytd_percentage >= 100 
    then 'On Track' 
    else 'Off Track' 
end ytd_status, 
case when l12m_percentage >= 100 
    then 'On Track' 
    else 'Off Track' 
end l12m_status, 
cast(ytd_actual / decode(ytd.billable_hours_per_year,0,1,ytd.billable_hours_per_year ) *100 as number(38,2)) as progress, 
ytd.dts_Updated_at as last_updated
from ytd 
left join last_12_mths on ytd.ukg_employee_number = last_12_mths.ukg_employee_number