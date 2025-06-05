{% test employee_source_count_match_sf(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE SRC_SYS_KEY = 'sfc'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('salesforce', 'contact') }} 
        WHERE _fivetran_deleted = false AND record_type_id = '0124W000001bI6LQAU'
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
