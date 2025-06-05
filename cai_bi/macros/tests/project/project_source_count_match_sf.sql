{% test project_source_count_match_sf(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE src_sys_key = 'sfc'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('salesforce', 'pse_proj_c') }} WHERE _fivetran_deleted = false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
