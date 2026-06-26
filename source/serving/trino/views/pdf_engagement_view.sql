CREATE OR REPLACE VIEW delta.mooc.pdf_engagement_view AS
SELECT
    event_date,
    course_id,
    user_id,
    content_id,
    chapter,
    event_count,
    distinct_sessions,
    scroll_count,
    zoom_count,
    scroll_up_count,
    scroll_down_count,
    distinct_pages,
    avg_page_number,
    max_page_number,
    min_page_number,
    avg_scale_amount,
    scroll_balance AS avg_scroll_balance,
    first_time,
    last_time,
    last_event_time
FROM delta.mooc.pdf_engagement_features
