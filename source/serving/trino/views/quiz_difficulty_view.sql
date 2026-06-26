CREATE OR REPLACE VIEW delta.mooc.quiz_difficulty_view AS
SELECT
    event_date,
    course_id,
    user_id,
    problem_id,
    problem_type,
    event_count,
    distinct_sessions,
    submit_count,
    check_count,
    graded_count,
    avg_weighted_earned,
    avg_weighted_possible,
    avg_score_ratio,
    max_score_ratio,
    min_score_ratio,
    first_time,
    last_time,
    last_event_time,
    attempt_count AS avg_attempt_count
FROM delta.mooc.quiz_attempt_metrics
