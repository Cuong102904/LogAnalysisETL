CREATE OR REPLACE VIEW delta.mooc.exam_question_difficulty_view AS
SELECT
    event_date,
    course_id,
    exam_id,
    exam_name,
    exam_attempt_id,
    user_id,
    problem_id,
    module_usage_key,
    module_display_name,
    CASE
        WHEN module_display_name IS NOT NULL AND trim(module_display_name) <> '' THEN module_display_name
        ELSE problem_id
    END AS question_label,
    CASE
        WHEN module_display_name IS NOT NULL AND trim(module_display_name) <> ''
            THEN concat(module_display_name, ' [', substr(problem_id, length(problem_id) - 3, 4), ']')
        ELSE problem_id
    END AS question_key,
    window_start_utc,
    window_end_utc,
    is_submitted,
    submission_event_count,
    max_attempt_no,
    first_submission_time_utc,
    last_submission_time_utc,
    grade_event_count,
    correct_grade_event_count,
    wrong_grade_event_count,
    COALESCE(avg_grade_ratio, 0e0) AS avg_grade_ratio,
    COALESCE(min_grade_ratio, 0e0) AS min_grade_ratio,
    COALESCE(max_grade_ratio, 0e0) AS max_grade_ratio,
    COALESCE(final_is_correct, false) AS final_is_correct,
    COALESCE(final_grade_ratio, 0e0) AS final_grade_ratio,
    final_grade_time_utc,
    first_grade_time_utc,
    last_grade_time_utc,
    CASE
        WHEN first_submission_time_utc IS NOT NULL AND final_grade_time_utc IS NOT NULL
            THEN date_diff('second', first_submission_time_utc, final_grade_time_utc)
        ELSE NULL
    END AS grading_latency_s
FROM delta.mooc.gold_exam_question_metrics
