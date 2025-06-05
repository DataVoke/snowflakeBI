{% test employee_source_count_match_sage(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE SRC_SYS_KEY = 'int'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('sage_intacct', 'employee') }} WHERE _fivetran_deleted = false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
