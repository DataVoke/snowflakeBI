{{ config(
    materialized = 'table',
    schema = 'dataconsumption',
    alias="forex_metrics"
) }}
select
    current_timestamp as dts_created_at,
    '{{ this.name }}' as created_by,
    current_timestamp as dts_updated_at,
    '{{ this.name }}' as updated_by,
    substr(currency_pair_name, 0, 3) as to_curr, substr(currency_pair_name, 5) as frm_curr,
    fm.*
from {{ source('forex_tracking_currency_exchange_rates_by_day', 'forex_metrics') }} fm
