{% test project_source_count_match_psa(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE src_sys_key = 'psa'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('psatools', 'projects') }} WHERE _fivetran_deleted = false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
