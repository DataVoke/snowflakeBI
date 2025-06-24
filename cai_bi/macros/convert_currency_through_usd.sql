{% macro convert_currency_through_usd(value_column, frm_curr_column, to_curr_column, date_column, forex_table) %}
  (
    CASE
      WHEN {{ frm_curr_column }} = {{ to_curr_column }} THEN {{ value_column }}
      ELSE
        -- Step 1: Convert from FROM_CURRENCY to USD
        {{ value_column }} / NULLIF(
          (
            SELECT close
            FROM {{ forex_table }} ex1
            WHERE ex1.frm_curr = {{ frm_curr_column }}
              AND ex1.to_curr = 'USD'
              AND ex1.run_date <= {{ date_column }}
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY ex1.frm_curr, ex1.to_curr ORDER BY ex1.run_date DESC
            ) = 1
          ), 0
        )
        *
        -- Step 2: Convert from USD to TO_CURRENCY
        NULLIF(
          (
            SELECT close
            FROM {{ forex_table }} ex2
            WHERE ex2.frm_curr = 'USD'
              AND ex2.to_curr = {{ to_curr_column }}
              AND ex2.run_date <= {{ date_column }}
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY ex2.frm_curr, ex2.to_curr ORDER BY ex2.run_date DESC
            ) = 1
          ), 0
        )
    END
  )
{% endmacro %}