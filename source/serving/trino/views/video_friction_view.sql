CREATE OR REPLACE VIEW delta.mooc.video_friction_view AS
SELECT
    event_date,
    course_id,
    video_id,
    SUM(event_count) AS event_count,
    MAX(distinct_users) AS distinct_users,
    MAX(distinct_sessions) AS distinct_sessions,
    SUM(CASE WHEN action_type IN ('pause', 'stop') THEN event_count ELSE 0 END) AS pause_stop_count,
    SUM(CASE WHEN action_type = 'seek' THEN event_count ELSE 0 END) AS seek_count,
    SUM(CASE WHEN action_type = 'play' THEN event_count ELSE 0 END) AS play_count,
    AVG(avg_watch_ratio) AS avg_watch_ratio,
    MAX(z_score) AS max_z_score,
    MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS has_anomaly
FROM delta.mooc.video_friction_signals
GROUP BY 1, 2, 3
