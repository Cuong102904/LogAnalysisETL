from __future__ import annotations

import http.client
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "delta")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "mooc")
BOOTSTRAP_TIMEOUT_SECONDS = int(os.getenv("TRINO_BOOTSTRAP_TIMEOUT_SECONDS", "600"))
BOOTSTRAP_DIR = Path("/bootstrap")
VIEWS_DIR = Path(os.getenv("TRINO_VIEWS_DIR", "/views"))
if not VIEWS_DIR.exists():
    VIEWS_DIR = Path(__file__).resolve().parents[1] / "views"

SEMANTIC_VIEW_CONTRACTS: dict[str, dict[str, list[str]]] = {
    "video_friction_view.sql": {
        "view_name": "video_friction_view",
        "tables": ["video_friction_signals"],
        "columns": [
            "event_date",
            "course_id",
            "video_id",
            "event_count",
            "distinct_users",
            "distinct_sessions",
            "pause_stop_count",
            "seek_count",
            "play_count",
            "avg_watch_ratio",
            "max_z_score",
            "has_anomaly",
        ],
    },
    "exam_anomaly_view.sql": {
        "view_name": "exam_anomaly_view",
        "tables": ["exam_integrity_signals"],
        "columns": [
            "event_date",
            "course_id",
            "exam_id",
            "attempt_id",
            "attempt_user_id",
            "username",
            "attempt_duration_secs",
            "security_event_count",
            "distinct_security_sessions",
            "distinct_login_ips",
            "proctoring_event_count",
            "z_score",
            "is_anomaly",
            "anomaly_type",
        ],
    },
    "pdf_engagement_view.sql": {
        "view_name": "pdf_engagement_view",
        "tables": ["pdf_engagement_features"],
        "columns": [
            "event_date",
            "course_id",
            "content_id",
            "chapter",
            "event_count",
            "scroll_count",
            "zoom_count",
            "scroll_up_count",
            "scroll_down_count",
            "avg_scale_amount",
            "avg_scroll_balance",
        ],
    },
    "quiz_difficulty_view.sql": {
        "view_name": "quiz_difficulty_view",
        "tables": ["quiz_attempt_metrics"],
        "columns": [
            "event_date",
            "course_id",
            "problem_id",
            "problem_type",
            "event_count",
            "submit_count",
            "check_count",
            "graded_count",
            "avg_score_ratio",
            "max_score_ratio",
            "min_score_ratio",
            "avg_attempt_count",
        ],
    },
    "course_improvement_view.sql": {
        "view_name": "course_improvement_view",
        "tables": [
            "user_learning_profile_daily",
            "video_friction_signals",
            "pdf_engagement_features",
            "quiz_attempt_metrics",
            "behavior_anomaly_signals",
        ],
        "columns": [
            "event_date",
            "course_id",
            "active_learners",
            "journey_events",
            "completion_events",
            "avg_event_span_minutes",
            "avg_video_share",
            "avg_pdf_share",
            "avg_performance_share",
            "avg_navigation_share",
            "video_events",
            "pause_events",
            "seek_events",
            "avg_watch_ratio",
            "max_video_z_score",
            "pdf_events",
            "scroll_events",
            "zoom_events",
            "avg_scroll_balance",
            "quiz_events",
            "submit_events",
            "avg_score_ratio",
            "max_score_ratio",
            "min_score_ratio",
            "anomaly_count",
        ],
    },
    "learner_health_view.sql": {
        "view_name": "learner_health_view",
        "tables": ["user_learning_profile_daily"],
        "columns": [
            "event_date",
            "course_id",
            "user_id",
            "event_count",
            "distinct_sessions",
            "video_event_count",
            "pdf_event_count",
            "performance_event_count",
            "navigation_event_count",
            "completion_event_count",
            "event_span_minutes",
            "engagement_score",
            "stuck_risk_band",
        ],
    },
    "behavior_anomaly_view.sql": {
        "view_name": "behavior_anomaly_view",
        "tables": ["behavior_anomaly_signals"],
        "columns": [
            "anomaly_domain",
            "entity_type",
            "entity_id",
            "event_date",
            "course_id",
            "metric_name",
            "metric_value",
            "event_count",
            "distinct_users",
            "distinct_sessions",
            "rolling_mean_7",
            "rolling_std_7",
            "z_score",
            "is_anomaly",
        ],
    },
    "alert_events_view.sql": {
        "view_name": "alert_events_view",
        "tables": ["anomaly_alerts"],
        "columns": [
            "alert_id",
            "alert_domain",
            "entity_type",
            "entity_id",
            "event_date",
            "course_id",
            "metric_name",
            "metric_value",
            "event_count",
            "distinct_users",
            "distinct_sessions",
            "alert_severity",
            "alert_type",
            "alert_message",
            "alert_time",
        ],
    },
}


def _endpoint(path: str) -> str:
    return f"http://{TRINO_HOST}:{TRINO_PORT}{path}"


def _http_request(
    method: str,
    url: str,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, timeout=15) as response:
        return response.status, dict(response.headers), response.read()


def wait_for_trino() -> None:
    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            status, _, body = _http_request("GET", _endpoint("/v1/info"))
            if status == 200:
                payload = json.loads(body.decode("utf-8"))
                if payload.get("starting") is False:
                    try:
                        execute_sql("SHOW SCHEMAS FROM delta")
                        return
                    except (urllib.error.HTTPError, RuntimeError) as exc:
                        if "SERVER_STARTING_UP" not in str(exc):
                            raise
        except Exception:
            time.sleep(2)
            continue
        time.sleep(2)
    raise RuntimeError("Timed out waiting for Trino to become ready")


def execute_sql(sql: str) -> None:
    headers = {
        "X-Trino-User": "bootstrap",
        "X-Trino-Catalog": TRINO_CATALOG,
        "X-Trino-Schema": TRINO_SCHEMA,
        "Content-Type": "text/plain; charset=utf-8",
    }
    status, _, body = _http_request(
        "POST", _endpoint("/v1/statement"), data=sql.encode("utf-8"), headers=headers
    )
    if status != 200:
        raise RuntimeError(f"Trino statement failed with status {status}: {body.decode('utf-8')}")
    payload = json.loads(body.decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(json.dumps(payload["error"], sort_keys=True))
    next_uri = payload.get("nextUri")
    while next_uri:
        try:
            _, _, body = _http_request(
                "GET", next_uri, headers={"X-Trino-User": "bootstrap"}
            )
        except (http.client.RemoteDisconnected, urllib.error.URLError, TimeoutError, ConnectionResetError):
            time.sleep(1)
            continue

        payload = json.loads(body.decode("utf-8"))
        if payload.get("error"):
            raise RuntimeError(json.dumps(payload["error"], sort_keys=True))
        next_uri = payload.get("nextUri")


def _is_missing_dependency_error(exc: Exception) -> bool:
    message = str(exc).lower()
    missing_markers = (
        "delta log",
        "does not exist",
        "doesn't exist",
        "not found",
        "no transaction log found",
        "table not found",
        "table location does not exist",
        "no such file",
        "accessdeniedexception",
        "access denied",
        "403 forbidden",
        "forbidden",
        "failed to create external path",
        "name or service not known",
        "temporary failure in name resolution",
        "unknownhostexception",
    )
    return any(marker in message for marker in missing_markers)


def _is_already_exists_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "already exists" in message or '"name": "already_exists"' in message


def register_tables(schema_name: str, register_statements: list[tuple[str, str]]) -> None:
    execute_sql(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{schema_name}")

    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    for table_name, table_location in register_statements:
        sql = (
            "CALL delta.system.register_table("
            f"schema_name => '{schema_name}', "
            f"table_name => '{table_name}', "
            f"table_location => '{table_location}')"
        )
        while True:
            try:
                execute_sql(sql)
                break
            except (urllib.error.HTTPError, RuntimeError) as exc:
                if _is_already_exists_error(exc):
                    print(f"skipping table registration for {schema_name}.{table_name}: already registered")
                    break
                if not _is_missing_dependency_error(exc):
                    raise RuntimeError(f"Failed to register {schema_name}.{table_name}") from exc
                if time.time() >= deadline:
                    print(
                        f"skipping table registration for {schema_name}.{table_name}: "
                        f"{table_location} is not materialized yet"
                    )
                    break
                time.sleep(5)


def validate_semantic_views() -> None:
    for sql_file_name, contract in SEMANTIC_VIEW_CONTRACTS.items():
        sql_file = VIEWS_DIR / sql_file_name
        if not sql_file.exists():
            raise RuntimeError(f"Missing semantic view SQL file: {sql_file_name}")

        sql = sql_file.read_text(encoding="utf-8").lower()
        expected_view_name = contract["view_name"].lower()
        if f"create or replace view delta.mooc.{expected_view_name}" not in sql:
            raise RuntimeError(f"{sql_file_name} does not define {expected_view_name}")
        if "silver_" in sql:
            raise RuntimeError(f"{sql_file_name} still references silver tables")
        for table_name in contract["tables"]:
            if f"delta.mooc.{table_name.lower()}" not in sql:
                raise RuntimeError(f"{sql_file_name} is missing dependency {table_name}")
        for column_name in contract["columns"]:
            if column_name.lower() not in sql:
                raise RuntimeError(f"{sql_file_name} is missing column contract {column_name}")


def register_direct_tables() -> None:
    direct_tables = [
        ("bronze_events", "s3://lakehouse/learnlake/bronze/bronze_events"),
        ("silver_event_index", "s3://lakehouse/learnlake/silver/silver_event_index"),
        ("silver_assessment_events", "s3://lakehouse/learnlake/silver/silver_assessment_events"),
        ("silver_auth_events", "s3://lakehouse/learnlake/silver/silver_auth_events"),
        ("silver_authoring_events", "s3://lakehouse/learnlake/silver/silver_authoring_events"),
        (
            "silver_course_content_events",
            "s3://lakehouse/learnlake/silver/silver_course_content_events",
        ),
        ("silver_document_events", "s3://lakehouse/learnlake/silver/silver_document_events"),
        ("silver_exam_events", "s3://lakehouse/learnlake/silver/silver_exam_events"),
        ("silver_invalid_events", "s3://lakehouse/learnlake/silver/silver_invalid_events"),
        (
            "silver_navigation_events",
            "s3://lakehouse/learnlake/silver/silver_navigation_events",
        ),
        ("silver_video_events", "s3://lakehouse/learnlake/silver/silver_video_events"),
        ("silver_system_events", "s3://lakehouse/learnlake/silver/silver_system_events"),
        ("silver_unknown_events", "s3://lakehouse/learnlake/silver/silver_unknown_events"),
    ]
    register_tables(TRINO_SCHEMA, direct_tables)


def register_gold_tables() -> None:
    register_statements = [
        (
            "video_friction_signals",
            "s3://lakehouse/learnlake/gold/video_friction_signals",
        ),
        (
            "pdf_engagement_features",
            "s3://lakehouse/learnlake/gold/pdf_engagement_features",
        ),
        (
            "quiz_attempt_metrics",
            "s3://lakehouse/learnlake/gold/quiz_attempt_metrics",
        ),
        (
            "user_learning_profile_daily",
            "s3://lakehouse/learnlake/gold/user_learning_profile_daily",
        ),
        (
            "exam_integrity_signals",
            "s3://lakehouse/learnlake/gold/exam_integrity_signals",
        ),
        (
            "behavior_anomaly_signals",
            "s3://lakehouse/learnlake/gold/behavior_anomaly_signals",
        ),
        ("anomaly_alerts", "s3://lakehouse/learnlake/gold/anomaly_alerts"),
    ]
    register_tables(TRINO_SCHEMA, register_statements)


def apply_views() -> None:
    validate_semantic_views()
    for sql_file in sorted(VIEWS_DIR.glob("*.sql")):
        if sql_file.name.startswith("00_"):
            continue
        sql = sql_file.read_text(encoding="utf-8").strip()
        if sql:
            try:
                execute_sql(sql)
            except (urllib.error.HTTPError, RuntimeError) as exc:
                if _is_missing_dependency_error(exc):
                    print(f"skipping view bootstrap for {sql_file.name}: dependency not ready")
                    continue
                raise


def main() -> None:
    wait_for_trino()
    register_direct_tables()
    register_gold_tables()
    apply_views()


if __name__ == "__main__":
    main()
