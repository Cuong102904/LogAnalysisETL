CREATE OR REPLACE VIEW delta.mooc.alert_events_view AS
SELECT
    alert_id,
    alert_domain,
    entity_type,
    entity_id,
    event_date,
    course_id,
    metric_name,
    metric_value,
    event_count,
    distinct_users,
    distinct_sessions,
    first_source_timestamp,
    last_source_timestamp,
    last_event_ts,
    rolling_mean_7,
    rolling_std_7,
    z_score,
    is_anomaly,
    alert_severity,
    alert_type,
    alert_message,
    alert_ts
FROM delta.mooc.anomaly_alerts
;
