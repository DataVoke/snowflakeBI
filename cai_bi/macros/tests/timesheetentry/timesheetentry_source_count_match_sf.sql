{% test timesheetentry_source_count_match_sf(model) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }} WHERE src_sys_key = 'sfc'
    ),
    source_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ source('salesforce', 'pse_task_time_c') }} tt
        LEFT JOIN {{ source('salesforce', 'pse_timecard_header_c') }} th
            ON tt.pse_timecard_c = th.id
        WHERE tt.is_deleted = false
          AND th.is_deleted = false
          AND (
              nullif(tt.pse_sunday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_monday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_tuesday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_wednesday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_thursday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_friday_hours_c, 0) IS NOT NULL OR
              nullif(tt.pse_saturday_hours_c, 0) IS NOT NULL
          )
    )
    SELECT 1
    WHERE (SELECT cnt FROM model_count) != (SELECT cnt FROM source_count)
{% endtest %}
