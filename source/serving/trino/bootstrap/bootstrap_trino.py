from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

TRINO_BASE_URL = os.environ.get("TRINO_BOOTSTRAP_URL", os.environ.get("TRINO_URL", "http://trino:8080"))
TRINO_USER = os.environ.get("TRINO_BOOTSTRAP_USER", "trino-bootstrap")
TRINO_SOURCE = os.environ.get("TRINO_BOOTSTRAP_SOURCE", "trino-bootstrap")
TRINO_CATALOG = os.environ.get("TRINO_BOOTSTRAP_CATALOG", "delta")
TRINO_SCHEMA = os.environ.get("TRINO_BOOTSTRAP_SCHEMA", "mooc")
TRINO_VIEWS_DIR = Path(os.environ.get("TRINO_VIEWS_DIR", "/views"))
TRINO_BOOTSTRAP_TABLE_LOCATION_SCHEME = os.environ.get("TRINO_BOOTSTRAP_TABLE_LOCATION_SCHEME", "s3a")
TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT = os.environ.get(
    "TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT",
    f"{TRINO_BOOTSTRAP_TABLE_LOCATION_SCHEME}://lakehouse/learnlake",
)

BOOTSTRAP_TIMEOUT_SECONDS = int(os.environ.get("TRINO_BOOTSTRAP_TIMEOUT_SECONDS", "300"))
BOOTSTRAP_POLL_SECONDS = float(os.environ.get("TRINO_BOOTSTRAP_POLL_SECONDS", "2"))
HTTP_TIMEOUT_SECONDS = float(os.environ.get("TRINO_HTTP_TIMEOUT_SECONDS", "10"))


@dataclass(frozen=True)
class TableRegistration:
    name: str
    location: str
    layer: str


REGISTERED_TABLES: tuple[TableRegistration, ...] = (
    TableRegistration(
        "bronze_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/bronze/bronze_events/",
        "bronze",
    ),
    TableRegistration(
        "events_canonical",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/events_canonical/",
        "silver",
    ),
    TableRegistration(
        "problem_submissions",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/problem_submissions/",
        "silver",
    ),
    TableRegistration(
        "problem_grades",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/problem_grades/",
        "silver",
    ),
    TableRegistration(
        "exam_attempts",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/exam_attempts/",
        "silver",
    ),
    TableRegistration(
        "video_interactions",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/video_interactions/",
        "silver",
    ),
    TableRegistration(
        "navigation_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/navigation_events/",
        "silver",
    ),
    TableRegistration(
        "content_access_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/content_access_events/",
        "silver",
    ),
    TableRegistration(
        "system_noise_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/system_noise_events/",
        "silver",
    ),
    TableRegistration(
        "silver_unknown_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/silver_unknown_events/",
        "silver",
    ),
    TableRegistration(
        "silver_invalid_events",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/silver/silver_invalid_events/",
        "silver",
    ),
    TableRegistration(
        "gold_exam_load_10s",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/gold/gold_exam_load_10s/",
        "gold",
    ),
    TableRegistration(
        "gold_exam_attempt_flow_10s",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/gold/gold_exam_attempt_flow_10s/",
        "gold",
    ),
    TableRegistration(
        "gold_exam_attempt_timeline",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/gold/gold_exam_attempt_timeline/",
        "gold",
    ),
    TableRegistration(
        "gold_exam_question_metrics",
        f"{TRINO_BOOTSTRAP_TABLE_LOCATION_ROOT}/gold/gold_exam_question_metrics/",
        "gold",
    ),
)

SEMANTIC_VIEWS: tuple[str, ...] = (
    "video_friction_view",
    "exam_anomaly_view",
    "pdf_engagement_view",
    "quiz_difficulty_view",
    "course_improvement_view",
    "learner_health_view",
    "behavior_anomaly_view",
    "alert_events_view",
)


class TrinoBootstrapError(RuntimeError):
    pass


class TrinoHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        user: str,
        source: str,
        timeout_seconds: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.source = source
        self.timeout_seconds = timeout_seconds

    def wait_until_ready(self, timeout_seconds: int, poll_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self._request_json("GET", "/v1/info")
                return
            except Exception as exc:  # pragma: no cover - network timing
                last_error = exc
                time.sleep(poll_seconds)
        raise TrinoBootstrapError(f"Trino did not become ready within {timeout_seconds}s") from last_error

    def query(self, statement: str, *, catalog: str | None = None, schema: str | None = None) -> list[dict[str, object]]:
        payload = statement.encode("utf-8")
        headers = self._statement_headers(catalog=catalog, schema=schema)
        response = self._request_json("POST", "/v1/statement", data=payload, headers=headers)
        rows: list[dict[str, object]] = []
        columns: list[dict[str, object]] | None = None

        while True:
            if response.get("error"):
                error = response["error"]
                raise TrinoBootstrapError(self._format_error(statement, error))

            if columns is None and response.get("columns"):
                columns = list(response["columns"])

            rows.extend(self._rows_from_response(response, columns))

            next_uri = response.get("nextUri")
            if not next_uri:
                return rows

            response = self._request_json_from_absolute("GET", next_uri, headers=headers)

    def execute(self, statement: str, *, catalog: str | None = None, schema: str | None = None) -> None:
        self.query(statement, catalog=catalog, schema=schema)

    def fetch_single_column(self, statement: str, *, catalog: str | None = None, schema: str | None = None) -> list[str]:
        rows = self.query(statement, catalog=catalog, schema=schema)
        if not rows:
            return []
        first_row = rows[0]
        if len(first_row) != 1:
            raise TrinoBootstrapError(f"Expected one column, got {len(first_row)} for statement: {statement}")
        return [str(next(iter(row.values()))) for row in rows]

    def _statement_headers(self, *, catalog: str | None, schema: str | None) -> dict[str, str]:
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Source": self.source,
            "Content-Type": "text/plain; charset=utf-8",
        }
        if catalog:
            headers["X-Trino-Catalog"] = catalog
        if schema:
            headers["X-Trino-Schema"] = schema
        return headers

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers or {},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise TrinoBootstrapError(f"Trino HTTP {exc.code} on {method} {path}: {body}") from exc
        except urllib.error.URLError as exc:
            raise TrinoBootstrapError(f"Trino request failed on {method} {path}: {exc}") from exc

    def _request_json_from_absolute(
        self,
        method: str,
        uri: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        request = urllib.request.Request(uri, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise TrinoBootstrapError(f"Trino HTTP {exc.code} on {method} {uri}: {body}") from exc
        except urllib.error.URLError as exc:
            raise TrinoBootstrapError(f"Trino request failed on {method} {uri}: {exc}") from exc

    @staticmethod
    def _rows_from_response(
        response: dict[str, object],
        columns: Sequence[dict[str, object]] | None,
    ) -> list[dict[str, object]]:
        data = response.get("data")
        if not data:
            return []
        if not columns:
            return []
        column_names = [str(column["name"]) for column in columns]
        rows: list[dict[str, object]] = []
        for raw_row in data:  # type: ignore[assignment]
            if not isinstance(raw_row, list):
                continue
            rows.append({column_names[index]: raw_row[index] for index in range(min(len(column_names), len(raw_row)))})
        return rows

    @staticmethod
    def _format_error(statement: str, error: object) -> str:
        if isinstance(error, dict):
            message = error.get("message", "unknown error")
            error_name = error.get("errorName") or error.get("errorCode")
            return f"{error_name}: {message}\nStatement: {statement}"
        return f"Trino error: {error}\nStatement: {statement}"


def validate_semantic_views(views_dir: Path | None = None) -> None:
    _validate_registry()
    if views_dir is not None and views_dir.exists():
        _validate_view_files(views_dir)


def main() -> None:
    attempts = int(os.environ.get("TRINO_BOOTSTRAP_ATTEMPTS", "5"))
    retry_delay_seconds = float(os.environ.get("TRINO_BOOTSTRAP_RETRY_DELAY_SECONDS", "2"))
    post_ready_delay_seconds = float(os.environ.get("TRINO_BOOTSTRAP_POST_READY_DELAY_SECONDS", "5"))

    validate_semantic_views()
    last_error: TrinoBootstrapError | None = None
    for attempt in range(1, attempts + 1):
        try:
            client = TrinoHttpClient(
                TRINO_BASE_URL,
                user=TRINO_USER,
                source=TRINO_SOURCE,
                timeout_seconds=HTTP_TIMEOUT_SECONDS,
            )
            client.wait_until_ready(BOOTSTRAP_TIMEOUT_SECONDS, BOOTSTRAP_POLL_SECONDS)
            time.sleep(post_ready_delay_seconds)
            bootstrap_schema(client)
            bootstrap_tables(client)
            bootstrap_views(client, TRINO_VIEWS_DIR)
            print("Trino bootstrap completed successfully")
            return
        except TrinoBootstrapError as exc:
            last_error = exc
            if attempt >= attempts:
                raise
            if "SERVER_STARTING_UP" not in str(exc):
                raise
            time.sleep(retry_delay_seconds)
    if last_error is not None:
        raise last_error


def bootstrap_schema(client: TrinoHttpClient) -> None:
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}", catalog=TRINO_CATALOG)


def bootstrap_tables(client: TrinoHttpClient) -> None:
    existing_tables = set(
        client.fetch_single_column(f"SHOW TABLES FROM {TRINO_CATALOG}.{TRINO_SCHEMA}", catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)
    )
    for table in REGISTERED_TABLES:
        if table.name in existing_tables:
            print(f"Skipping already registered table {TRINO_CATALOG}.{TRINO_SCHEMA}.{table.name}")
            continue
        register_table(client, table)
        print(f"Registered {table.layer} table {TRINO_CATALOG}.{TRINO_SCHEMA}.{table.name}")
        existing_tables.add(table.name)


def register_table(client: TrinoHttpClient, table: TableRegistration) -> None:
    statement = (
        f"CALL {TRINO_CATALOG}.system.register_table("
        f"schema_name => '{TRINO_SCHEMA}', "
        f"table_name => '{table.name}', "
        f"table_location => '{table.location}')"
    )
    client.execute(statement, catalog=TRINO_CATALOG, schema="system")


def bootstrap_views(client: TrinoHttpClient, views_dir: Path) -> None:
    if not views_dir.exists():
        print(f"Views directory not found at {views_dir}; skipping semantic view bootstrap")
        return

    sql_files = sorted(path for path in views_dir.glob("*.sql") if path.is_file())
    if not sql_files:
        print(f"No SQL files found in {views_dir}; skipping semantic view bootstrap")
        return

    for sql_file in sql_files:
        statement = _read_sql_file(sql_file)
        if not statement:
            continue
        client.execute(statement, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)
        print(f"Applied semantic SQL from {sql_file.name}")


def _read_sql_file(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in _split_sql_statements(content) if statement.strip()]
    if not statements:
        return ""
    if len(statements) > 1:
        raise TrinoBootstrapError(f"{path} contains multiple SQL statements; bootstrap expects one statement per file")
    return statements[0]


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    length = len(sql)

    while i < length:
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)
            i += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single_quote and not in_double_quote and char == "-" and next_char == "-":
            in_line_comment = True
            i += 2
            continue

        if not in_single_quote and not in_double_quote and char == "/" and next_char == "*":
            in_block_comment = True
            i += 2
            continue

        if char == "'" and not in_double_quote:
            buffer.append(char)
            if in_single_quote and next_char == "'":
                buffer.append(next_char)
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue

        if char == '"' and not in_single_quote:
            buffer.append(char)
            in_double_quote = not in_double_quote
            i += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def _validate_registry() -> None:
    if not REGISTERED_TABLES:
        raise TrinoBootstrapError("No tables configured for Trino bootstrap")
    if len({table.name for table in REGISTERED_TABLES}) != len(REGISTERED_TABLES):
        raise TrinoBootstrapError("Duplicate table names detected in Trino bootstrap registry")
    if len({table.location for table in REGISTERED_TABLES}) != len(REGISTERED_TABLES):
        raise TrinoBootstrapError("Duplicate table locations detected in Trino bootstrap registry")
    if not SEMANTIC_VIEWS:
        raise TrinoBootstrapError("No semantic views configured for Trino bootstrap")
    if len(set(SEMANTIC_VIEWS)) != len(SEMANTIC_VIEWS):
        raise TrinoBootstrapError("Duplicate semantic view names detected in Trino bootstrap registry")
    missing_view_suffix = [name for name in SEMANTIC_VIEWS if not name.endswith("_view")]
    if missing_view_suffix:
        raise TrinoBootstrapError(f"Semantic views must use *_view naming: {missing_view_suffix}")


def _validate_view_files(views_dir: Path) -> None:
    sql_files = [path for path in views_dir.glob("*.sql") if path.is_file()]
    if not sql_files:
        return

    expected_names = set(SEMANTIC_VIEWS)
    found_names = {path.stem for path in sql_files}

    missing = sorted(expected_names - found_names)
    unexpected = sorted(found_names - expected_names)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected: {', '.join(unexpected)}")
        raise TrinoBootstrapError(f"Semantic view file mismatch in {views_dir}: {'; '.join(details)}")


if __name__ == "__main__":
    main()
