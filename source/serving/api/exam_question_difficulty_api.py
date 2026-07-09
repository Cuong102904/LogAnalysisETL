from __future__ import annotations

import json
import os
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "superset")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "delta")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "mooc")
QUESTION_VIEW = os.environ.get("EXAM_QUESTION_DIFFICULTY_VIEW", "exam_question_difficulty_view")
API_HOST = os.environ.get("EXAM_QUESTION_API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("EXAM_QUESTION_API_PORT", "8091"))


@dataclass(frozen=True)
class QueryFilters:
    course_id: str | None
    exam_id: str | None
    exam_name: str | None
    start_date: str | None
    end_date: str | None
    min_attempts: int
    limit: int


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_int(value: str | None, *, default: int, minimum: int, maximum: int) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"Expected integer value, got {value!r}") from exc
    return max(minimum, min(parsed, maximum))


def parse_filters(query_params: dict[str, list[str]]) -> QueryFilters:
    def first(name: str) -> str | None:
        values = query_params.get(name, [])
        if not values:
            return None
        value = values[0].strip()
        return value or None

    return QueryFilters(
        course_id=first("course_id"),
        exam_id=first("exam_id"),
        exam_name=first("exam_name"),
        start_date=first("start_date"),
        end_date=first("end_date"),
        min_attempts=_parse_int(first("min_attempts"), default=5, minimum=1, maximum=100000),
        limit=_parse_int(first("limit"), default=20, minimum=1, maximum=100),
    )


def build_question_difficulty_query(filters: QueryFilters) -> str:
    where_clauses = ["1 = 1"]
    if filters.course_id:
        where_clauses.append(f"course_id = {_sql_string(filters.course_id)}")
    if filters.exam_id:
        where_clauses.append(f"exam_id = {_sql_string(filters.exam_id)}")
    if filters.exam_name:
        where_clauses.append(f"exam_name = {_sql_string(filters.exam_name)}")
    if filters.start_date:
        where_clauses.append(f"event_date >= DATE {_sql_string(filters.start_date)}")
    if filters.end_date:
        where_clauses.append(f"event_date <= DATE {_sql_string(filters.end_date)}")

    return f"""
SELECT
    problem_id,
    question_label,
    MAX(question_key) AS question_key,
    MAX(module_display_name) AS module_display_name,
    COUNT(DISTINCT exam_attempt_id) AS attempt_count,
    SUM(CASE WHEN final_is_correct THEN 1 ELSE 0 END) AS correct_attempt_count,
    SUM(CASE WHEN NOT final_is_correct THEN 1 ELSE 0 END) AS wrong_attempt_count,
    AVG(final_grade_ratio) AS avg_final_grade_ratio,
    AVG(CAST(submission_event_count AS DOUBLE)) AS avg_submission_event_count,
    AVG(CAST(max_attempt_no AS DOUBLE)) AS avg_attempt_no,
    AVG(CAST(grading_latency_s AS DOUBLE)) AS avg_grading_latency_s
FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{QUESTION_VIEW}
WHERE {" AND ".join(where_clauses)}
GROUP BY problem_id, question_label
HAVING COUNT(DISTINCT exam_attempt_id) >= {filters.min_attempts}
ORDER BY avg_final_grade_ratio ASC, wrong_attempt_count DESC, attempt_count DESC
LIMIT {filters.limit}
""".strip()


def run_query(statement: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    import trino

    connection = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        description = cursor.description or []
        columns = [column[0] for column in description]
        return columns, rows
    finally:
        connection.close()


def serialize_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for index, column_name in enumerate(columns):
            item[column_name] = row[index]
        serialized.append(item)
    return serialized


class ExamQuestionDifficultyHandler(BaseHTTPRequestHandler):
    server_version = "ExamQuestionDifficultyAPI/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._write_json(HTTPStatus.OK, {"status": "ok"})
            return

        if parsed.path != "/api/v1/exam-question-difficulty":
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            return

        try:
            filters = parse_filters(parse_qs(parsed.query, keep_blank_values=False))
            statement = build_question_difficulty_query(filters)
            columns, rows = run_query(statement)
        except ValueError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:  # pragma: no cover - runtime/network branch
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        payload = {
            "filters": {
                "course_id": filters.course_id,
                "exam_id": filters.exam_id,
                "exam_name": filters.exam_name,
                "start_date": filters.start_date,
                "end_date": filters.end_date,
                "min_attempts": filters.min_attempts,
                "limit": filters.limit,
            },
            "rows": serialize_rows(columns, rows),
            "row_count": len(rows),
        }
        self._write_json(HTTPStatus.OK, payload)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True, default=str).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer((API_HOST, API_PORT), ExamQuestionDifficultyHandler)
    print(json.dumps({"host": API_HOST, "port": API_PORT, "view": QUESTION_VIEW}, ensure_ascii=True))
    server.serve_forever()


if __name__ == "__main__":
    main()
