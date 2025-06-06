{% test timesheet_source_count_match_sage(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE src_sys_key = 'int'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt FROM {{ source('sage_intacct', 'timesheetentry') }} WHERE _fivetran_deleted = false
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
