CREATE OR REPLACE VIEW delta.mooc.pdf_engagement_view AS
SELECT
    event_date,
    course_id,
    content_id,
    chapter,
    SUM(event_count) AS event_count,
    SUM(distinct_sessions) AS distinct_sessions,
    SUM(scroll_count) AS scroll_count,
    SUM(zoom_count) AS zoom_count,
    SUM(scroll_up_count) AS scroll_up_count,
    SUM(scroll_down_count) AS scroll_down_count,
    AVG(avg_scale_amount) AS avg_scale_amount,
    AVG(scroll_balance) AS avg_scroll_balance
FROM delta.mooc.pdf_engagement_features
GROUP BY 1, 2, 3, 4;
