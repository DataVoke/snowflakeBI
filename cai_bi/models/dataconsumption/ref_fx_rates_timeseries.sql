{{ config(materialized='table', schema='dataconsumption', alias='fx_rates_timeseries') }}

WITH fx AS (
  SELECT
     base_currency_id AS to_curr,
     quote_currency_id AS frm_curr,
     base_currency_name AS to_currency_name,
     quote_currency_name AS frm_currency_name,
     date,
     value
    FROM {{ source('finance_economics', 'fx_rates_timeseries') }}
    WHERE date > '2016-01-01'
      AND base_currency_id = 'USD'
),

inverted_fx AS (
    SELECT
     f1.frm_curr AS frm_curr,
     f2.frm_curr AS to_curr,
     f1.date AS date,
     CAST((1 / f1.value) * f2.value AS number(38, 17)) AS fx_rate_mul,
     CAST(1 / ((1 / f1.value) * f2.value) AS number(38, 17)) AS fx_rate_div
    FROM fx f1
    JOIN fx f2
      ON f1.frm_curr != f2.frm_curr AND f1.date = f2.date
),

direct_fx AS (
    SELECT
     frm_curr,
     to_curr,
     date,
     CAST(fx_rate_div AS number(38,17)) AS fx_rate_div,
     CAST(fx_rate_mul AS number(38,17)) AS fx_rate_mul
    FROM inverted_fx

    UNION ALL

    SELECT
     frm_curr,
     to_curr,
     date,
     CAST(value AS number(38,17)) AS fx_rate_div,
     CAST(1/value AS number(38,17)) AS fx_rate_mul
    FROM fx

    UNION ALL

    SELECT
     to_curr AS frm_curr,
     frm_curr AS to_curr,
     date,
     CAST(1/value AS number(38,17)) AS fx_rate_div,
     CAST(value AS number(38,17)) AS fx_rate_mul
    FROM fx
),

-- Determine the latest date in your FX data and today's date
dates AS (
    SELECT
        MAX(date) AS latest_fx_date,
        CURRENT_DATE() AS today
    FROM fx
),

-- Cap calendar at min(latest_fx_date, today) to avoid future dates
calendar AS (
    SELECT
        dateadd(day, seq4(), '2016-01-01') AS calendar_date
    FROM table(generator(rowcount => 3650))
    CROSS JOIN dates
    WHERE dateadd(day, seq4(), '2016-01-01') <= LEAST(dates.latest_fx_date, dates.today)
),

-- All unique currency pairs
pairs AS (
    SELECT DISTINCT frm_curr, to_curr
    FROM direct_fx
),

-- Cross join currency pairs with calendar to get every date for every pair
expanded AS (
    SELECT
     p.frm_curr,
     p.to_curr,
     c.calendar_date AS date
    FROM pairs p
    CROSS JOIN calendar c
),

-- Join actual rates and prepare to forward-fill
joined AS (
    SELECT
     e.frm_curr,
     e.to_curr,
     e.date,
     d.fx_rate_div,
     d.fx_rate_mul
    FROM expanded e
    LEFT JOIN direct_fx d
      ON e.frm_curr = d.frm_curr
     AND e.to_curr = d.to_curr
     AND e.date = d.date
),

-- Forward-fill missing rates with the last known prior rate
filled AS (
    SELECT
     frm_curr,
     to_curr,
     date,
     LAST_VALUE(fx_rate_div IGNORE NULLS) OVER (
         PARTITION BY frm_curr, to_curr
         ORDER BY date
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ) AS fx_rate_div,
     LAST_VALUE(fx_rate_mul IGNORE NULLS) OVER (
         PARTITION BY frm_curr, to_curr
         ORDER BY date
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ) AS fx_rate_mul
    FROM joined
)

SELECT
    frm_curr,
    to_curr,
    date,
    fx_rate_div,
    fx_rate_mul
FROM filled
where fx_rate_div is not null
ORDER BY frm_curr, to_curr, date