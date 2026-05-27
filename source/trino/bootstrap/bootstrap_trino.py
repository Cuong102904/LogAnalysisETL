from __future__ import annotations

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
VIEWS_DIR = Path("/views")


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
            status, _, _ = _http_request("GET", _endpoint("/v1/info"))
            if status == 200:
                return
        except Exception:
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
        _, _, body = _http_request("GET", next_uri, headers={"X-Trino-User": "bootstrap"})
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
        "table not found",
        "table location does not exist",
        "no such file",
    )
    return any(marker in message for marker in missing_markers)


def register_gold_tables() -> None:
    execute_sql(
        "CREATE SCHEMA IF NOT EXISTS delta.mooc WITH (location = 's3://lakehouse/mooc/gold')"
    )
    register_statements = [
        (
            "silver_video_interactions",
            "s3://lakehouse/mooc/silver/video_interactions",
        ),
        (
            "video_friction_signals",
            "s3://lakehouse/mooc/gold/video_friction_signals",
        ),
        (
            "pdf_engagement_features",
            "s3://lakehouse/mooc/gold/pdf_engagement_features",
        ),
        (
            "quiz_attempt_metrics",
            "s3://lakehouse/mooc/gold/quiz_attempt_metrics",
        ),
        (
            "user_learning_profile_daily",
            "s3://lakehouse/mooc/gold/user_learning_profile_daily",
        ),
        (
            "exam_integrity_signals",
            "s3://lakehouse/mooc/gold/exam_integrity_signals",
        ),
        (
            "behavior_anomaly_signals",
            "s3://lakehouse/mooc/gold/behavior_anomaly_signals",
        ),
        ("anomaly_alerts", "s3://lakehouse/mooc/gold/anomaly_alerts"),
    ]

    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    for table_name, table_location in register_statements:
        sql = (
            "CALL delta.system.register_table("
            f"schema_name => '{TRINO_SCHEMA}', "
            f"table_name => '{table_name}', "
            f"table_location => '{table_location}')"
        )
        while True:
            try:
                execute_sql(sql)
                break
            except (urllib.error.HTTPError, RuntimeError) as exc:
                if not _is_missing_dependency_error(exc):
                    raise RuntimeError(f"Failed to register {table_name}") from exc
                if time.time() >= deadline:
                    print(
                        f"skipping table registration for {table_name}: "
                        f"{table_location} is not materialized yet"
                    )
                    break
                time.sleep(5)


def apply_views() -> None:
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
    register_gold_tables()
    apply_views()


if __name__ == "__main__":
    main()
