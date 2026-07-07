from __future__ import annotations

from pathlib import Path

from apps.maintenance.filter_bk_activity_logs import filter_bk_activity_logs


def test_filter_bk_activity_logs_keeps_matching_records(tmp_path) -> None:
    input_root = tmp_path / "BK_activity_logs_unzipped"
    output_root = tmp_path / "data_filter"
    input_file = input_root / "normal_days" / "tracking.log-20260101-123.jsonl"
    input_file.parent.mkdir(parents=True, exist_ok=True)
    input_file.write_text(
        "\n".join(
            [
                '{"time":"2026-01-01T10:00:30Z","event_type":"seq_next","event_source":"browser","username":"learner_1","session":"sess-1","agent":"Mozilla/5.0","context":{"user_id":101,"course_id":"course-v1:BK+TEST+2026","org_id":"BK","path":"/courses/course-v1:BK+TEST+2026/courseware/unit/1"}}',
                '{"time":"2026-01-01T10:00:10Z","event_type":"play_video","event_source":"browser","username":"learner_1","session":"sess-1","agent":"Mozilla/5.0","context":{"user_id":101,"course_id":"course-v1:BK+TEST+2026","org_id":"BK","path":"/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@abc/handler/play"}}',
                '{"time":"2026-01-01T10:01:00Z","event_type":"edx.grades.problem.submitted","event_source":"server","username":"learner_2","session":"sess-2","agent":"Mozilla/5.0","context":{"user_id":102,"course_id":"course-v1:BK+TEST+2026","org_id":"BK","path":"/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check"}}',
                '{"time":"2026-01-01T10:02:00Z","event_type":"/heartbeat","event_source":"server","username":"","session":"sess-noise","agent":"Googlebot","context":{"course_id":"course-v1:BK+TEST+2026","org_id":"BK","path":"/heartbeat"}}',
                '{"time":"2026-01-01T10:03:00Z","event_type":"unknown.custom.event","event_source":"server","username":"learner_3","session":"sess-3","agent":"Mozilla/5.0","context":{"user_id":103,"course_id":"course-v1:BK+TEST+2026","org_id":"BK","path":"/courses/course-v1:BK+TEST+2026/unknown"}}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    stats = filter_bk_activity_logs(
        input_root,
        output_root,
        Path("projects/daotao_ai/routing.yaml"),
    )

    output_file = output_root / "normal_days" / "tracking.log-20260101-123.jsonl"
    assert stats["files"] == 1
    assert stats["matched"] == 4
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8").splitlines() == input_file.read_text(
        encoding="utf-8"
    ).splitlines()[:4]
