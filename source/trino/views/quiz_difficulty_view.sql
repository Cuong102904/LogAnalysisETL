CREATE OR REPLACE VIEW delta.mooc.quiz_difficulty_view AS
SELECT
    event_date,
    course_id,
    problem_id,
    problem_type,
    SUM(event_count) AS event_count,
    SUM(distinct_sessions) AS distinct_sessions,
    SUM(submit_count) AS submit_count,
    SUM(check_count) AS check_count,
    SUM(graded_count) AS graded_count,
    AVG(avg_score_ratio) AS avg_score_ratio,
    MAX(max_score_ratio) AS max_score_ratio,
    MIN(min_score_ratio) AS min_score_ratio,
    AVG(attempt_count) AS avg_attempt_count
FROM delta.mooc.quiz_attempt_metrics
GROUP BY 1, 2, 3, 4;
