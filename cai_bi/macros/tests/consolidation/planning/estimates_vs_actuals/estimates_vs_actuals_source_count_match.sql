{% test estimates_vs_actuals_source_count_match(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE src_sys_key = 'sfc'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('salesforce', 'pse_est_vs_actuals_c') }} WHERE is_deleted = false and _fivetran_deleted=false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
