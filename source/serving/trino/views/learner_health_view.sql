CREATE OR REPLACE VIEW delta.mooc.learner_health_view AS
SELECT
    event_date,
    course_id,
    user_id,
    event_count,
    distinct_sessions,
    video_event_count,
    pdf_event_count,
    performance_event_count,
    navigation_event_count,
    completion_event_count,
    event_span_minutes,
    video_share,
    pdf_share,
    performance_share,
    navigation_share,
    CASE
        WHEN event_count IS NULL OR event_count = 0 THEN 0
        ELSE ROUND(
            (
                COALESCE(video_share, 0) * 2
                + COALESCE(pdf_share, 0) * 2
                + COALESCE(performance_share, 0) * 3
                + COALESCE(navigation_share, 0)
                + COALESCE(completion_event_count, 0) / CAST(event_count AS DOUBLE)
            ) * 100,
            2
        )
    END AS engagement_score,
    CASE
        WHEN COALESCE(video_share, 0) < 0.2 AND COALESCE(pdf_share, 0) < 0.2 AND COALESCE(performance_share, 0) < 0.2 THEN 'high'
        WHEN COALESCE(event_span_minutes, 0) < 5 AND COALESCE(completion_event_count, 0) = 0 THEN 'medium'
        ELSE 'low'
    END AS stuck_risk_band
FROM delta.mooc.user_learning_profile_daily
