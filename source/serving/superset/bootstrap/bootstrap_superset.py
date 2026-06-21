from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

SUPERSET_HOST = os.getenv("SUPERSET_HOST", "superset")
SUPERSET_PORT = int(os.getenv("SUPERSET_PORT", "8088"))
SUPERSET_ADMIN_USERNAME = os.getenv("SUPERSET_ADMIN_USERNAME", "superset_admin")
SUPERSET_ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "change-me")
SUPERSET_ADMIN_EMAIL = os.getenv("SUPERSET_ADMIN_EMAIL", "admin@example.com")
SUPERSET_TRINO_SQLALCHEMY_URI = os.getenv(
    "SUPERSET_TRINO_SQLALCHEMY_URI",
    "trino://superset@trino:8080/delta/mooc",
)
BOOTSTRAP_TIMEOUT_SECONDS = int(os.getenv("SUPERSET_BOOTSTRAP_TIMEOUT_SECONDS", "600"))


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    schema: str
    table_name: str
    columns: list[str]


DATASET_SPECS: list[DatasetSpec] = [
    DatasetSpec(
        name="video_friction_view",
        schema="mooc",
        table_name="video_friction_view",
        columns=[
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
    ),
    DatasetSpec(
        name="exam_anomaly_view",
        schema="mooc",
        table_name="exam_anomaly_view",
        columns=[
            "event_date",
            "course_id",
            "exam_id",
            "attempt_id",
            "attempt_user_id",
            "username",
            "attempt_duration_secs",
            "distinct_login_ips",
            "proctoring_event_count",
            "z_score",
            "is_anomaly",
            "anomaly_type",
        ],
    ),
    DatasetSpec(
        name="pdf_engagement_view",
        schema="mooc",
        table_name="pdf_engagement_view",
        columns=[
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
    ),
    DatasetSpec(
        name="quiz_difficulty_view",
        schema="mooc",
        table_name="quiz_difficulty_view",
        columns=[
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
    ),
    DatasetSpec(
        name="course_improvement_view",
        schema="mooc",
        table_name="course_improvement_view",
        columns=[
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
            "pdf_events",
            "quiz_events",
            "anomaly_count",
        ],
    ),
    DatasetSpec(
        name="learner_health_view",
        schema="mooc",
        table_name="learner_health_view",
        columns=[
            "event_date",
            "course_id",
            "user_id",
            "event_count",
            "completion_event_count",
            "event_span_minutes",
            "engagement_score",
            "stuck_risk_band",
        ],
    ),
    DatasetSpec(
        name="behavior_anomaly_view",
        schema="mooc",
        table_name="behavior_anomaly_view",
        columns=[
            "event_date",
            "course_id",
            "anomaly_domain",
            "entity_type",
            "entity_id",
            "event_count",
            "distinct_users",
            "distinct_sessions",
            "z_score",
            "is_anomaly",
        ],
    ),
    DatasetSpec(
        name="alert_events_view",
        schema="mooc",
        table_name="alert_events_view",
        columns=[
            "event_date",
            "course_id",
            "alert_domain",
            "entity_type",
            "entity_id",
            "metric_name",
            "metric_value",
            "event_count",
            "distinct_users",
            "z_score",
            "alert_severity",
            "alert_type",
        ],
    ),
]


def _base_url() -> str:
    return f"http://{SUPERSET_HOST}:{SUPERSET_PORT}"


def _endpoint(path: str) -> str:
    return f"{_base_url()}{path}"


def _http_request(
    method: str,
    url: str,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, dict(response.headers), response.read()


def _json_request(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    body = None
    request_headers = {"Content-Type": "application/json", **(headers or {})}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
    status, _, raw = _http_request(method, url, data=body, headers=request_headers)
    if raw:
        parsed = json.loads(raw.decode("utf-8"))
    else:
        parsed = None
    return status, parsed


def wait_for_superset() -> None:
    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            status, _ = _json_request("GET", _endpoint("/health"))
            if status == 200:
                return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Timed out waiting for Superset to become ready")


def login() -> str:
    status, payload = _json_request(
        "POST",
        _endpoint("/api/v1/security/login"),
        payload={
            "username": SUPERSET_ADMIN_USERNAME,
            "password": SUPERSET_ADMIN_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
    )
    if status != 200:
        raise RuntimeError(f"Superset login failed: {payload}")
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Missing access token in login response: {payload}")
    return token


def authed_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def slugify(value: str) -> str:
    normalized = []
    last_dash = False
    for char in value.lower():
        if char.isalnum():
            normalized.append(char)
            last_dash = False
        elif not last_dash:
            normalized.append("-")
            last_dash = True
    slug = "".join(normalized).strip("-")
    return slug or "dashboard"


def get_or_create_database(access_token: str) -> int:
    headers = authed_headers(access_token)
    _, payload = _json_request("GET", _endpoint("/api/v1/database/"), headers=headers)
    for item in (payload or {}).get("result", []):
        if item.get("database_name") == "trino_delta":
            return int(item["id"])

    create_payload = {
        "database_name": "trino_delta",
        "sqlalchemy_uri": SUPERSET_TRINO_SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "impersonate_user": False,
        "configuration_method": "sqlalchemy_form",
        "extra": json.dumps({"allow_multi_catalog": True, "disable_data_preview": True}),
    }
    status, created = _json_request(
        "POST", _endpoint("/api/v1/database/"), payload=create_payload, headers=headers
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to create Superset database: {created}")
    if isinstance(created, dict) and created.get("id"):
        return int(created["id"])

    _, payload = _json_request("GET", _endpoint("/api/v1/database/"), headers=headers)
    for item in (payload or {}).get("result", []):
        if item.get("database_name") == "trino_delta":
            return int(item["id"])
    raise RuntimeError("Could not resolve database id for trino_delta")


def get_or_create_dataset(access_token: str, database_id: int, spec: DatasetSpec) -> dict[str, Any]:
    headers = authed_headers(access_token)
    status, payload = _json_request(
        "POST",
        _endpoint("/api/v1/dataset/get_or_create/"),
        payload={
            "database_id": database_id,
            "schema": spec.schema,
            "table_name": spec.table_name,
            "always_filter_main_dttm": False,
            "normalize_columns": False,
        },
        headers=headers,
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to get_or_create dataset {spec.name}: {payload}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected dataset response for {spec.name}: {payload}")
    return payload


def list_dashboards(access_token: str) -> list[dict[str, Any]]:
    _, payload = _json_request(
        "GET", _endpoint("/api/v1/dashboard/"), headers=authed_headers(access_token)
    )
    return list((payload or {}).get("result", []))


def get_or_create_dashboard(access_token: str, title: str) -> dict[str, Any]:
    headers = authed_headers(access_token)
    slug = slugify(title)
    for item in list_dashboards(access_token):
        if item.get("dashboard_title") == title or item.get("slug") == slug:
            return item

    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "json_metadata": json.dumps(
            {
                "color_scheme": "supersetColors",
                "expanded_slices": {},
                "refresh_frequency": 0,
            }
        ),
        "position_json": json.dumps(
            {
                "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
                "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
            }
        ),
        "css": "",
    }
    status, created = _json_request(
        "POST", _endpoint("/api/v1/dashboard/"), payload=payload, headers=headers
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to create dashboard {title}: {created}")
    if isinstance(created, dict):
        return created
    raise RuntimeError(f"Unexpected dashboard response: {created}")


def list_charts(access_token: str) -> list[dict[str, Any]]:
    _, payload = _json_request(
        "GET", _endpoint("/api/v1/chart/"), headers=authed_headers(access_token)
    )
    return list((payload or {}).get("result", []))


def get_chart_by_name(access_token: str, slice_name: str) -> dict[str, Any] | None:
    for item in list_charts(access_token):
        if item.get("slice_name") == slice_name:
            return item
    return None


def create_chart(
    access_token: str,
    dataset: dict[str, Any],
    dashboard_id: int,
    chart_name: str,
    columns: list[str],
) -> dict[str, Any]:
    headers = authed_headers(access_token)
    existing = get_chart_by_name(access_token, chart_name)
    if existing:
        return existing

    params = {
        "datasource": f"{dataset['id']}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "slice_id": None,
        "all_columns": columns,
        "row_limit": 50,
        "order_desc": True,
        "show_cell_bars": False,
    }
    payload = {
        "slice_name": chart_name,
        "datasource_id": int(dataset["id"]),
        "datasource_type": "table",
        "viz_type": "table",
        "dashboards": [dashboard_id],
        "params": json.dumps(params),
        "query_context_generation": True,
        "description": f"Auto-generated chart for {dataset['table_name']}",
    }
    status, created = _json_request(
        "POST", _endpoint("/api/v1/chart/"), payload=payload, headers=headers
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to create chart {chart_name}: {created}")
    if not isinstance(created, dict):
        raise RuntimeError(f"Unexpected chart response for {chart_name}: {created}")
    return created


def get_chart_detail(access_token: str, chart_id: int) -> dict[str, Any]:
    _, payload = _json_request(
        "GET", _endpoint(f"/api/v1/chart/{chart_id}"), headers=authed_headers(access_token)
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected chart detail for {chart_id}: {payload}")
    return payload


def build_position_json(charts: list[dict[str, Any]]) -> str:
    position: dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }
    for index, chart in enumerate(charts, start=1):
        node_id = f"CHART-{index}"
        position["GRID_ID"]["children"].append(node_id)
        position[node_id] = {
            "type": "CHART",
            "id": node_id,
            "children": [],
            "meta": {
                "chartId": int(chart["id"]),
                "uuid": chart["uuid"],
                "sliceName": chart["slice_name"],
                "width": 12,
                "height": 45,
            },
        }
    return json.dumps(position)


def update_dashboard_layout(
    access_token: str, dashboard: dict[str, Any], charts: list[dict[str, Any]]
) -> None:
    payload = {
        "dashboard_title": dashboard["dashboard_title"],
        "slug": dashboard.get("slug"),
        "published": True,
        "json_metadata": dashboard.get("json_metadata") or json.dumps({}),
        "position_json": build_position_json(charts),
        "css": dashboard.get("css", ""),
        "owners": [
            owner["id"]
            for owner in dashboard.get("owners", [])
            if isinstance(owner, dict) and owner.get("id")
        ],
    }
    status, updated = _json_request(
        "PUT",
        _endpoint(f"/api/v1/dashboard/{dashboard['id']}"),
        payload=payload,
        headers=authed_headers(access_token),
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to update dashboard layout: {updated}")


def main() -> None:
    wait_for_superset()
    token = login()
    database_id = get_or_create_database(token)
    dashboard = get_or_create_dashboard(token, "Behavior Intelligence")

    chart_specs = [
        ("Course Improvement", "course_improvement_view"),
        ("Learner Health", "learner_health_view"),
        ("Video Friction", "video_friction_view"),
        ("Exam Anomaly", "exam_anomaly_view"),
        ("Alert Events", "alert_events_view"),
    ]
    charts: list[dict[str, Any]] = []
    for chart_name, dataset_name in chart_specs:
        spec = next(item for item in DATASET_SPECS if item.name == dataset_name)
        dataset = get_or_create_dataset(token, database_id, spec)
        chart = create_chart(token, dataset, int(dashboard["id"]), chart_name, spec.columns)
        chart_detail = get_chart_detail(token, int(chart["id"]))
        charts.append(
            {
                "id": chart_detail["id"],
                "uuid": chart_detail["uuid"],
                "slice_name": chart_detail["slice_name"],
            }
        )

    update_dashboard_layout(token, dashboard, charts)


if __name__ == "__main__":
    main()
