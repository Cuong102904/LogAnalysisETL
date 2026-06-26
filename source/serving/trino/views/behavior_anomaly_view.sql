CREATE OR REPLACE VIEW delta.mooc.behavior_anomaly_view AS
SELECT
    anomaly_domain,
    entity_type,
    entity_id,
    event_date,
    course_id,
    metric_name,
    metric_value,
    event_count,
    distinct_users,
    distinct_sessions,
    first_time,
    last_time,
    last_event_time,
    rolling_mean_7,
    rolling_std_7,
    z_score,
    is_anomaly
FROM delta.mooc.behavior_anomaly_signals
