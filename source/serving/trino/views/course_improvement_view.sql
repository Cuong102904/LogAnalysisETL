CREATE OR REPLACE VIEW delta.mooc.course_improvement_view AS
WITH journey AS (
    SELECT
        event_date,
        course_id,
        COUNT(DISTINCT user_id) AS active_learners,
        SUM(event_count) AS journey_events,
        SUM(completion_event_count) AS completion_events,
        AVG(event_span_minutes) AS avg_event_span_minutes,
        AVG(video_share) AS avg_video_share,
        AVG(pdf_share) AS avg_pdf_share,
        AVG(performance_share) AS avg_performance_share,
        AVG(navigation_share) AS avg_navigation_share
    FROM delta.mooc.user_learning_profile_daily
    GROUP BY 1, 2
),
video AS (
    SELECT
        event_date,
        course_id,
        SUM(event_count) AS video_events,
        SUM(CASE WHEN action_type IN ('pause', 'stop') THEN event_count ELSE 0 END) AS pause_events,
        SUM(CASE WHEN action_type = 'seek' THEN event_count ELSE 0 END) AS seek_events,
        AVG(avg_watch_ratio) AS avg_watch_ratio,
        MAX(z_score) AS max_video_z_score
    FROM delta.mooc.video_friction_signals
    GROUP BY 1, 2
),
pdf AS (
    SELECT
        event_date,
        course_id,
        SUM(event_count) AS pdf_events,
        SUM(scroll_count) AS scroll_events,
        SUM(zoom_count) AS zoom_events,
        AVG(scroll_balance) AS avg_scroll_balance
    FROM delta.mooc.pdf_engagement_features
    GROUP BY 1, 2
),
quiz AS (
    SELECT
        event_date,
        course_id,
        SUM(event_count) AS quiz_events,
        SUM(submit_count) AS submit_events,
        AVG(avg_score_ratio) AS avg_score_ratio,
        MAX(max_score_ratio) AS max_score_ratio,
        MIN(min_score_ratio) AS min_score_ratio
    FROM delta.mooc.quiz_attempt_metrics
    GROUP BY 1, 2
),
anomaly AS (
    SELECT
        event_date,
        course_id,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
    FROM delta.mooc.behavior_anomaly_signals
    GROUP BY 1, 2
)
SELECT
    COALESCE(journey.event_date, video.event_date, pdf.event_date, quiz.event_date, anomaly.event_date) AS event_date,
    COALESCE(journey.course_id, video.course_id, pdf.course_id, quiz.course_id, anomaly.course_id) AS course_id,
    journey.active_learners,
    journey.journey_events,
    journey.completion_events,
    journey.avg_event_span_minutes,
    journey.avg_video_share,
    journey.avg_pdf_share,
    journey.avg_performance_share,
    journey.avg_navigation_share,
    video.video_events,
    video.pause_events,
    video.seek_events,
    video.avg_watch_ratio,
    video.max_video_z_score,
    pdf.pdf_events,
    pdf.scroll_events,
    pdf.zoom_events,
    pdf.avg_scroll_balance,
    quiz.quiz_events,
    quiz.submit_events,
    quiz.avg_score_ratio,
    quiz.max_score_ratio,
    quiz.min_score_ratio,
    anomaly.anomaly_count
FROM journey
FULL OUTER JOIN video
    ON journey.event_date = video.event_date
   AND journey.course_id = video.course_id
FULL OUTER JOIN pdf
    ON COALESCE(journey.event_date, video.event_date) = pdf.event_date
   AND COALESCE(journey.course_id, video.course_id) = pdf.course_id
FULL OUTER JOIN quiz
    ON COALESCE(journey.event_date, video.event_date, pdf.event_date) = quiz.event_date
   AND COALESCE(journey.course_id, video.course_id, pdf.course_id) = quiz.course_id
FULL OUTER JOIN anomaly
    ON COALESCE(journey.event_date, video.event_date, pdf.event_date, quiz.event_date) = anomaly.event_date
   AND COALESCE(journey.course_id, video.course_id, pdf.course_id, quiz.course_id) = anomaly.course_id
