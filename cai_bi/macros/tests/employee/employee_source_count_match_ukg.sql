{% test employee_source_count_match_ukg(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE SRC_SYS_KEY = 'ukg'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('ukg_pro', 'employee') }} WHERE _fivetran_deleted = false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
