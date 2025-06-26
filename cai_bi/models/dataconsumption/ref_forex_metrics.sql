{{ config(
    materialized = 'table',
    schema = 'dataconsumption',
    alias="forex_metrics"
) }}
WITH fx AS (
 select
substr(currency_pair_name, 0, 3) as to_curr, substr(currency_pair_name, 5) as frm_curr, close as  closing_rate, currency_pair_name, run_date 
from forex_tracking_currency_exchange_rates_by_day.stock.forex_metrics fm where run_date >= '2016-01-01' and to_curr ='USD' -- and to_curr = 'SEK' and frm_curr = 'CHF'  
qualify row_number() over (partition by to_curr,frm_curr, run_date order by run_date desc )),
inverted_fx AS (
  SELECT
    f1.frm_curr AS frm_curr,
    f2.frm_curr AS to_curr,
    f1.run_date run_date,
    cast((1 / f1.closing_rate) * f2.closing_rate as number(38,18) ) AS closing_rate_div,
    cast(1/closing_rate_div as number(38,18) ) as closing_rate_mul
  FROM fx f1
  JOIN fx f2 ON f1.frm_curr != f2.frm_curr and f1.run_date =f2.run_date
),
final as ( 
SELECT frm_curr,to_curr,run_date,closing_rate_div,closing_rate_mul
FROM inverted_fx
union
(select frm_curr,to_curr,run_date,cast(closing_rate as number(38,18) ) as closing_rate_div, cast(1/closing_rate_div as number(38,18)) as closing_rate_mul  from fx)
union
(select to_curr as frm_curr,frm_curr as to_curr, run_date,cast(1/closing_rate as number(38,18) )  as closing_rate_div, cast(1/closing_rate_div as number(38,18)) as closing_rate_mul from fx)
)
select 
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
* from final