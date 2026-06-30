from __future__ import annotations

import json
import os
import subprocess
import shutil
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
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


@dataclass(frozen=True)
class ChartSpec:
    title: str
    dataset_name: str
    viz_type: str
    form_data: dict[str, Any] = field(default_factory=dict)
    width: int = 12
    height: int = 45
    description: str | None = None


@dataclass(frozen=True)
class DashboardSectionSpec:
    title: str
    charts: tuple[ChartSpec, ...]


@dataclass(frozen=True)
class DashboardSurfaceSpec:
    title: str
    refresh_frequency: int
    sections: tuple[DashboardSectionSpec, ...]


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
            "security_event_count",
            "distinct_security_sessions",
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

DATASET_SPEC_BY_NAME = {spec.name: spec for spec in DATASET_SPECS}


def sql_metric(label: str, expression: str) -> dict[str, str]:
    return {
        "label": label,
        "expressionType": "SQL",
        "sqlExpression": expression,
    }


def adhoc_filter(column: str, operator: str, value: Any) -> dict[str, Any]:
    return {
        "clause": "WHERE",
        "expressionType": "SIMPLE",
        "subject": column,
        "operator": operator,
        "comparator": value,
    }


def time_series_form_data(
    *,
    time_column: str,
    metrics: list[dict[str, Any]],
    time_grain: str = "P1D",
    groupby: list[str] | None = None,
    row_limit: int = 50,
    chart_type: str = "line",
) -> dict[str, Any]:
    form_data: dict[str, Any] = {
        "granularity_sqla": time_column,
        "time_grain_sqla": time_grain,
        "time_range": "No filter",
        "metrics": metrics,
        "row_limit": row_limit,
        "groupby": groupby or [],
        "adhoc_filters": [],
    }
    if chart_type == "bar":
        form_data["show_brush"] = False
        form_data["orientation"] = "horizontal"
    return form_data


def bar_form_data(
    *,
    groupby: list[str],
    metrics: list[dict[str, Any]],
    row_limit: int = 10,
) -> dict[str, Any]:
    return {
        "groupby": groupby,
        "metrics": metrics,
        "adhoc_filters": [],
        "row_limit": row_limit,
        "time_range": "No filter",
        "show_brush": False,
        "orientation": "horizontal",
    }


def big_number_form_data(
    *,
    metric: dict[str, Any],
    adhoc_filters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "metric": metric,
        "adhoc_filters": adhoc_filters or [],
        "time_range": "No filter",
    }


def matrix_form_data(
    *,
    rows: list[str],
    columns: list[str],
    metric: dict[str, Any],
    row_limit: int = 50,
) -> dict[str, Any]:
    return {
        "groupby": rows,
        "columns": columns,
        "metrics": [metric],
        "adhoc_filters": [],
        "row_limit": row_limit,
        "time_range": "No filter",
        "transpose_pivot": False,
        "combine_metric": False,
    }


def table_form_data(*, columns: list[str], row_limit: int = 50) -> dict[str, Any]:
    return {
        "all_columns": columns,
        "row_limit": row_limit,
        "order_desc": True,
        "show_cell_bars": False,
    }

LIVE_OPS_SURFACE = DashboardSurfaceSpec(
    title="Live Ops",
    refresh_frequency=30,
    sections=(
        DashboardSectionSpec(
            title="Operational Overview",
            charts=(
                ChartSpec(
                    "Active Alerts",
                    "alert_events_view",
                    "big_number_total",
                    big_number_form_data(metric=sql_metric("Alert Count", "COUNT(*)")),
                    width=4,
                    height=22,
                ),
                ChartSpec(
                    "Open Anomalies",
                    "behavior_anomaly_view",
                    "big_number_total",
                    big_number_form_data(
                        metric=sql_metric("Anomaly Count", "COUNT(*)"),
                        adhoc_filters=[adhoc_filter("is_anomaly", "==", True)],
                    ),
                    width=4,
                    height=22,
                ),
                ChartSpec(
                    "Exam Flags",
                    "exam_anomaly_view",
                    "big_number_total",
                    big_number_form_data(
                        metric=sql_metric("Exam Flag Count", "COUNT(*)"),
                        adhoc_filters=[adhoc_filter("is_anomaly", "==", True)],
                    ),
                    width=4,
                    height=22,
                ),
            ),
        ),
        DashboardSectionSpec(
            title="Trend Watch",
            charts=(
                ChartSpec(
                    "Video Friction Trend",
                    "video_friction_view",
                    "echarts_timeseries_line",
                    time_series_form_data(
                        time_column="event_date",
                        metrics=[
                            sql_metric("Avg Watch Ratio", "AVG(avg_watch_ratio)"),
                            sql_metric("Pause / Stop Events", "SUM(pause_stop_count)"),
                        ],
                    ),
                    height=48,
                ),
                ChartSpec(
                    "Exam Integrity Trend",
                    "exam_anomaly_view",
                    "echarts_timeseries_bar",
                    time_series_form_data(
                        time_column="event_date",
                        metrics=[
                            sql_metric("Security Events", "SUM(security_event_count)"),
                            sql_metric("Proctoring Events", "SUM(proctoring_event_count)"),
                        ],
                        chart_type="bar",
                    ),
                    height=48,
                ),
            ),
        ),
        DashboardSectionSpec(
            title="Triage",
            charts=(
                ChartSpec(
                    "Behavior Heatmap",
                    "behavior_anomaly_view",
                    "pivot_table",
                    matrix_form_data(
                        rows=["anomaly_domain"],
                        columns=["entity_type"],
                        metric=sql_metric("Anomaly Count", "COUNT(*)"),
                        row_limit=25,
                    ),
                    height=50,
                ),
                ChartSpec(
                    "Alert Events",
                    "alert_events_view",
                    "table",
                    table_form_data(
                        columns=[
                            "event_date",
                            "course_id",
                            "alert_domain",
                            "entity_type",
                            "entity_id",
                            "metric_name",
                            "metric_value",
                            "event_count",
                            "z_score",
                            "alert_severity",
                            "alert_type",
                        ],
                        row_limit=100,
                    ),
                    height=55,
                ),
            ),
        ),
    ),
)

LEARNING_ANALYTICS_SURFACE = DashboardSurfaceSpec(
    title="Learning Analytics",
    refresh_frequency=0,
    sections=(
        DashboardSectionSpec(
            title="Course Overview",
            charts=(
                ChartSpec(
                    "Course Improvement",
                    "course_improvement_view",
                    "echarts_timeseries_line",
                    time_series_form_data(
                        time_column="event_date",
                        metrics=[
                            sql_metric("Active Learners", "AVG(active_learners)"),
                            sql_metric("Completion Events", "SUM(completion_events)"),
                            sql_metric("Avg Watch Ratio", "AVG(avg_watch_ratio)"),
                        ],
                    ),
                    height=50,
                ),
                ChartSpec(
                    "Learner Health",
                    "learner_health_view",
                    "big_number_total",
                    big_number_form_data(
                        metric=sql_metric("Average Engagement", "AVG(engagement_score)"),
                    ),
                    width=4,
                    height=22,
                ),
                ChartSpec(
                    "Risk Mix",
                    "learner_health_view",
                    "echarts_bar",
                    bar_form_data(
                        groupby=["stuck_risk_band"],
                        metrics=[sql_metric("Learners", "COUNT(*)")],
                        row_limit=10,
                    ),
                    width=8,
                    height=36,
                ),
            ),
        ),
        DashboardSectionSpec(
            title="Content Drill-down",
            charts=(
                ChartSpec(
                    "PDF Engagement",
                    "pdf_engagement_view",
                    "echarts_bar",
                    bar_form_data(
                        groupby=["chapter"],
                        metrics=[
                            sql_metric("Scroll Events", "SUM(scroll_count)"),
                            sql_metric("Zoom Events", "SUM(zoom_count)"),
                        ],
                        row_limit=12,
                    ),
                    height=46,
                ),
                ChartSpec(
                    "Quiz Difficulty",
                    "quiz_difficulty_view",
                    "pivot_table",
                    matrix_form_data(
                        rows=["problem_type"],
                        columns=["course_id"],
                        metric=sql_metric("Average Score Ratio", "AVG(avg_score_ratio)"),
                        row_limit=30,
                    ),
                    height=48,
                ),
            ),
        ),
    ),
)

DASHBOARD_SURFACES: tuple[DashboardSurfaceSpec, ...] = (
    LIVE_OPS_SURFACE,
    LEARNING_ANALYTICS_SURFACE,
)


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
            status, _, _ = _http_request("GET", _endpoint("/health"))
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
    database_payload = {
        "database_name": "trino_delta",
        "sqlalchemy_uri": SUPERSET_TRINO_SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": True,
        "allow_file_upload": False,
        "impersonate_user": False,
        "configuration_method": "sqlalchemy_form",
        "extra": json.dumps({"allow_multi_catalog": True, "disable_data_preview": True}),
    }
    for item in (payload or {}).get("result", []):
        if item.get("database_name") == "trino_delta":
            database_id = int(item["id"])
            _json_request(
                "PUT",
                _endpoint(f"/api/v1/database/{database_id}"),
                payload=database_payload,
                headers=headers,
            )
            return int(item["id"])

    status, created = _json_request(
        "POST", _endpoint("/api/v1/database/"), payload=database_payload, headers=headers
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
    payload: Any = None
    for attempt in range(1, 9):
        try:
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
            if status in (200, 201):
                break
            raise RuntimeError(f"Failed to get_or_create dataset {spec.name}: {payload}")
        except urllib.error.HTTPError as exc:
            if exc.code != 422 or attempt == 8:
                raise
            time.sleep(2)
    else:
        raise RuntimeError(f"Failed to get_or_create dataset {spec.name}: {payload}")
    dataset_id: int | None = None
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, dict) and result.get("table_id") is not None:
            dataset_id = int(result["table_id"])
        elif payload.get("id") is not None:
            dataset_id = int(payload["id"])

    if dataset_id is None:
        _, datasets_payload = _json_request("GET", _endpoint("/api/v1/dataset/"), headers=headers)
        for item in (datasets_payload or {}).get("result", []):
            if (
                item.get("database", {}).get("id") == database_id
                and item.get("schema") == spec.schema
                and item.get("table_name") == spec.table_name
            ):
                return item
        raise RuntimeError(f"Unexpected dataset response for {spec.name}: {payload}")

    _, datasets_payload = _json_request("GET", _endpoint("/api/v1/dataset/"), headers=headers)
    for item in (datasets_payload or {}).get("result", []):
        if int(item.get("id", -1)) == dataset_id:
            return item
    raise RuntimeError(f"Could not resolve dataset id for {spec.name}: {payload}")


def list_dashboards(access_token: str) -> list[dict[str, Any]]:
    _, payload = _json_request(
        "GET", _endpoint("/api/v1/dashboard/"), headers=authed_headers(access_token)
    )
    return list((payload or {}).get("result", []))


def get_or_create_dashboard(access_token: str, title: str, refresh_frequency: int) -> dict[str, Any]:
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
                "refresh_frequency": refresh_frequency,
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
    if isinstance(created, dict) and created.get("id") is not None:
        for item in list_dashboards(access_token):
            if int(item.get("id", -1)) == int(created["id"]):
                return item
        if created.get("dashboard_title") is not None:
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
    chart_spec: ChartSpec,
) -> dict[str, Any]:
    headers = authed_headers(access_token)
    existing = get_chart_by_name(access_token, chart_spec.title)
    if existing:
        return existing

    params = {
        "datasource": f"{dataset['id']}__table",
        "viz_type": chart_spec.viz_type,
        "query_mode": "raw" if chart_spec.viz_type == "table" else "aggregate",
        "slice_id": None,
        **chart_spec.form_data,
    }
    payload = {
        "slice_name": chart_spec.title,
        "datasource_id": int(dataset["id"]),
        "datasource_type": "table",
        "viz_type": chart_spec.viz_type,
        "dashboards": [dashboard_id],
        "params": json.dumps(params),
        "query_context_generation": True,
        "description": chart_spec.description
        or f"Auto-generated {chart_spec.viz_type} chart for {dataset['table_name']}",
    }
    status, created = _json_request(
        "POST", _endpoint("/api/v1/chart/"), payload=payload, headers=headers
    )
    if status not in (200, 201):
        raise RuntimeError(f"Failed to create chart {chart_spec.title}: {created}")
    if not isinstance(created, dict):
        raise RuntimeError(f"Unexpected chart response for {chart_spec.title}: {created}")
    return created


def get_chart_detail(access_token: str, chart_id: int) -> dict[str, Any]:
    _, payload = _json_request(
        "GET", _endpoint(f"/api/v1/chart/{chart_id}"), headers=authed_headers(access_token)
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected chart detail for {chart_id}: {payload}")
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    return payload


def build_position_json(
    sections: tuple[DashboardSectionSpec, ...],
    charts_by_title: dict[str, dict[str, Any]],
) -> str:
    position: dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["TABS_ID"]},
        "TABS_ID": {"type": "TABS", "id": "TABS_ID", "children": []},
    }
    for section_index, section in enumerate(sections, start=1):
        tab_id = f"TAB-{section_index}"
        tab_children: list[str] = []
        for chart_index, chart_spec in enumerate(section.charts, start=1):
            chart_detail = charts_by_title.get(chart_spec.title)
            if not chart_detail:
                continue
            node_id = f"{tab_id}-CHART-{chart_index}"
            tab_children.append(node_id)
            position[node_id] = {
                "type": "CHART",
                "id": node_id,
                "children": [],
                "meta": {
                    "chartId": int(chart_detail["id"]),
                    "uuid": chart_detail.get("uuid") or f"chart-{chart_detail['id']}",
                    "sliceName": chart_detail["slice_name"],
                    "width": chart_spec.width,
                    "height": chart_spec.height,
                },
            }
        if tab_children:
            position["TABS_ID"]["children"].append(tab_id)
            position[tab_id] = {
                "type": "TAB",
                "id": tab_id,
                "children": tab_children,
                "meta": {"text": section.title, "title": section.title},
            }
    return json.dumps(position)


def update_dashboard_layout(
    access_token: str,
    dashboard: dict[str, Any],
    surface: DashboardSurfaceSpec,
    charts_by_title: dict[str, dict[str, Any]],
) -> None:
    payload = {
        "dashboard_title": dashboard["dashboard_title"],
        "slug": dashboard.get("slug"),
        "published": True,
        "json_metadata": json.dumps(
            {
                "color_scheme": "supersetColors",
                "expanded_slices": {},
                "refresh_frequency": surface.refresh_frequency,
            }
        ),
        "position_json": build_position_json(surface.sections, charts_by_title),
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


def build_bootstrap_registry() -> dict[str, Any]:
    return {
        "database": {
            "name": "trino_delta",
            "sqlalchemy_uri": SUPERSET_TRINO_SQLALCHEMY_URI,
        },
        "datasets": [
            {
                "name": spec.name,
                "schema": spec.schema,
                "table_name": spec.table_name,
                "columns": spec.columns,
            }
            for spec in DATASET_SPECS
        ],
        "surfaces": [
            {
                "title": surface.title,
                "refresh_frequency": surface.refresh_frequency,
                "sections": [
                    {
                        "title": section.title,
                        "charts": [
                            {
                                "title": chart.title,
                                "dataset_name": chart.dataset_name,
                                "viz_type": chart.viz_type,
                                "form_data": chart.form_data,
                                "width": chart.width,
                                "height": chart.height,
                            }
                            for chart in section.charts
                        ],
                    }
                    for section in surface.sections
                ],
            }
            for surface in DASHBOARD_SURFACES
        ],
    }


def run_playwright_fallback() -> None:
    script_path = Path(__file__).with_name("playwright_fallback.js")
    if not script_path.exists():
        raise RuntimeError(f"Missing Playwright fallback script: {script_path}")
    if shutil.which("playwright-cli") is None:
        print("Playwright CLI not available; skipping Superset fallback bootstrap")
        return

    rendered_script = (
        script_path.read_text(encoding="utf-8")
        .replace(
            "__SUPERSET_BOOTSTRAP_REGISTRY_JSON__",
            json.dumps(build_bootstrap_registry()),
        )
        .replace("__SUPERSET_BASE_URL__", json.dumps(_base_url()))
        .replace("__SUPERSET_ADMIN_USERNAME__", json.dumps(SUPERSET_ADMIN_USERNAME))
        .replace("__SUPERSET_ADMIN_PASSWORD__", json.dumps(SUPERSET_ADMIN_PASSWORD))
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as temp_file:
        temp_file.write(rendered_script)
        rendered_path = Path(temp_file.name)

    env = os.environ.copy()
    subprocess.run(["playwright-cli", "open"], check=True, env=env)
    try:
        subprocess.run(
            ["playwright-cli", "run-code", f"--filename={rendered_path}"],
            check=True,
            env=env,
        )
    finally:
        subprocess.run(["playwright-cli", "close"], check=False, env=env)
        rendered_path.unlink(missing_ok=True)


def bootstrap_via_api() -> None:
    token = login()
    database_id = get_or_create_database(token)
    for surface in DASHBOARD_SURFACES:
        dashboard: dict[str, Any] | None = None
        charts_by_title: dict[str, dict[str, Any]] = {}
        surface_failed = False
        for section in surface.sections:
            for chart_spec in section.charts:
                dataset_spec = DATASET_SPEC_BY_NAME[chart_spec.dataset_name]
                try:
                    dataset = get_or_create_dataset(token, database_id, dataset_spec)
                except Exception as exc:
                    print(
                        f"Skipping dataset {dataset_spec.name} for surface {surface.title}: {exc}"
                    )
                    continue

                if dashboard is None:
                    try:
                        dashboard = get_or_create_dashboard(
                            token, surface.title, surface.refresh_frequency
                        )
                    except Exception as exc:
                        print(f"Skipping surface {surface.title}: {exc}")
                        surface_failed = True
                        break

                try:
                    chart = create_chart(
                        token,
                        dataset,
                        int(dashboard["id"]),
                        chart_spec,
                    )
                    charts_by_title[chart_spec.title] = get_chart_detail(
                        token, int(chart["id"])
                    )
                except Exception as exc:
                    print(f"Skipping chart {chart_spec.title} on surface {surface.title}: {exc}")
                    continue
            if surface_failed:
                break

        if dashboard and charts_by_title:
            update_dashboard_layout(token, dashboard, surface, charts_by_title)


def main() -> None:
    wait_for_superset()
    try:
        bootstrap_via_api()
    except Exception as exc:
        print(f"Superset API bootstrap failed, falling back to Playwright: {exc}")
        run_playwright_fallback()


if __name__ == "__main__":
    main()
