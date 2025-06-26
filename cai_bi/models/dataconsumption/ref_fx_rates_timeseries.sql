{{ config(
    materialized = 'table',
    schema = 'dataconsumption',
    alias="fx_rates_timeseries"
) }}
WITH fx AS (
 select base_currency_id to_curr, quote_currency_id frm_curr,base_currency_name as to_currency_name,
 quote_currency_name as frm_currency_name,date, value  from {{ source('finance_economics', 'fx_rates_timeseries') }} 
 where date >'2016-01-01' and base_currency_id ='USD'
),
inverted_fx AS (
  SELECT
    f1.frm_curr AS frm_curr,
    f2.frm_curr AS to_curr,
    f2.frm_currency_name as to_currency_name,
    f1.frm_currency_name as frm_currency_name,
    f1.date date,
    cast((1 / f1.value) * f2.value as number(38,18) ) AS fx_rate_mul,
    1/fx_rate_mul as fx_rate_div
  FROM fx f1
  JOIN fx f2 ON f1.frm_curr != f2.frm_curr and f1.date =f2.date
)
SELECT frm_curr,to_curr,to_currency_name,frm_currency_name,date,fx_rate_div,fx_rate_mul
FROM inverted_fx
union
(select frm_curr,to_curr,to_currency_name,frm_currency_name,date,cast(value as number(38,18) ) as fx_rate_div, cast(1/fx_rate_div as number(38,18)) as fx_rate_mul  from fx)
union
(select to_curr as frm_curr,frm_curr as to_curr, frm_currency_name as to_currency_name ,to_currency_name as frm_currency_name,date,cast(1/value as number(38,18) )  as fx_rate_div, cast(1/fx_rate_div as number(38,18)) as fx_rate_mul from fx)