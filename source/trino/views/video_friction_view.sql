CREATE OR REPLACE VIEW delta.mooc.video_friction_view AS
WITH video_activity AS (
    SELECT
        event_date,
        course_id,
        video_id,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS distinct_users,
        COUNT(DISTINCT session_id) AS distinct_sessions,
        SUM(CASE WHEN action_type IN ('pause_video', 'stop_video') THEN 1 ELSE 0 END) AS pause_stop_count,
        SUM(CASE WHEN action_type = 'seek_video' THEN 1 ELSE 0 END) AS seek_count,
        SUM(CASE WHEN action_type = 'play_video' THEN 1 ELSE 0 END) AS play_count,
        AVG(
            CASE
                WHEN video_duration IS NOT NULL AND video_duration <> 0
                    THEN CAST(current_time AS double) / CAST(video_duration AS double)
            END
        ) AS avg_watch_ratio
    FROM delta.mooc.silver_video_interactions
    GROUP BY 1, 2, 3
),
video_anomaly AS (
    SELECT
        event_date,
        course_id,
        video_id,
        MAX(z_score) AS max_z_score,
        MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS has_anomaly
    FROM delta.mooc.video_friction_signals
    GROUP BY 1, 2, 3
)
SELECT
    activity.event_date,
    activity.course_id,
    activity.video_id,
    activity.event_count,
    activity.distinct_users,
    activity.distinct_sessions,
    activity.pause_stop_count,
    activity.seek_count,
    activity.play_count,
    activity.avg_watch_ratio,
    anomaly.max_z_score,
    COALESCE(anomaly.has_anomaly, 0) AS has_anomaly
FROM video_activity AS activity
LEFT JOIN video_anomaly AS anomaly
    ON activity.event_date = anomaly.event_date
   AND activity.course_id = anomaly.course_id
   AND activity.video_id = anomaly.video_id;
