from __future__ import annotations

import argparse
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "superset_admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "")
SUPERSET_PROVIDER = os.environ.get("SUPERSET_AUTH_PROVIDER", "db")
SUPERSET_DATABASE_NAME = os.environ.get("SUPERSET_TRINO_DATABASE_NAME", "trino_delta_mooc")
DEFAULT_SCHEMA = os.environ.get("SUPERSET_TRINO_SCHEMA", "mooc")
DEFAULT_CATALOG = os.environ.get("SUPERSET_TRINO_CATALOG", "delta")
DEFAULT_HOTSPOTS_TABLE = "gold_course_video_seek_hotspots_daily"
DEFAULT_SUMMARY_TABLE = "gold_course_video_summary_daily"
DEFAULT_RETENTION_TABLE = "gold_video_retention_by_bucket_daily"


class SupersetApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatasetRefs:
    hotspots: int
    summary: int
    retention: int


@dataclass(frozen=True)
class ChartRefs:
    heatmap: int
    direction: int
    summary_table: int
    retention: int


@dataclass(frozen=True)
class SupersetObjectIds:
    dashboard_id: int
    datasets: DatasetRefs
    charts: ChartRefs


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str, provider: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.provider = provider
        self.access_token = self._login()

    def _login(self) -> str:
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": self.provider,
            "refresh": True,
        }
        response = self.request("POST", "/api/v1/security/login", payload, include_auth=False)
        return response["access_token"]

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        *,
        include_auth: bool = True,
    ) -> dict[str, object]:
        headers: dict[str, str] = {}
        body: bytes | None = None
        if include_auth:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(f"{self.base_url}{path}", data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as response:
                text = response.read().decode("utf-8")
                return json.loads(text) if text else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SupersetApiError(f"{method} {path} failed with HTTP {exc.code}: {detail}") from exc

    def list_objects(self, resource: str, page_size: int = 100) -> list[dict[str, object]]:
        query = urllib.parse.quote(f"(page:0,page_size:{page_size})", safe="(),:")
        response = self.request("GET", f"/api/v1/{resource}/?q={query}")
        return list(response.get("result", []))


def _filter_clause(subject: str, comparator: object, operator: str, operator_id: str | None = None) -> dict[str, object]:
    clause: dict[str, object] = {
        "expressionType": "SIMPLE",
        "subject": subject,
        "operator": operator,
        "comparator": comparator,
        "clause": "WHERE",
        "sqlExpression": None,
        "isExtra": False,
        "isNew": False,
        "datasourceWarning": False,
    }
    if operator_id is not None:
        clause["operatorId"] = operator_id
    return clause


def _metric(column_name: str, aggregate: str = "SUM", *, label: str | None = None) -> dict[str, object]:
    metric_label = label or f"{aggregate}({column_name})"
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate,
        "sqlExpression": None,
        "datasourceWarning": False,
        "hasCustomLabel": label is not None,
        "label": metric_label,
        "optionName": f"metric_{aggregate.lower()}_{column_name}",
    }


def _table_column(label: str) -> dict[str, object]:
    return {
        "timeGrain": None,
        "columnType": "BASE_AXIS",
        "sqlExpression": label,
        "label": label,
        "expressionType": "SQL",
    }


def _no_time_filter() -> list[dict[str, object]]:
    return [_filter_clause("event_date", "No filter", "TEMPORAL_RANGE")]


def _heatmap_form_data(dataset_id: int) -> dict[str, object]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "heatmap_v2",
        "x_axis": "position_bucket_30s",
        "groupby": "video_code",
        "metric": _metric("seek_event_count"),
        "adhoc_filters": _no_time_filter(),
        "row_limit": 10000,
        "sort_x_axis": "alpha_asc",
        "sort_y_axis": "alpha_asc",
        "normalize_across": "heatmap",
        "legend_type": "continuous",
        "linear_color_scheme": "superset_seq_1",
        "left_margin": "auto",
        "bottom_margin": "auto",
        "show_legend": True,
        "show_percentage": False,
        "show_values": False,
        "normalized": False,
        "x_axis_title": "30s position bucket",
        "y_axis_title": "video",
        "extra_form_data": {},
        "dashboards": [],
    }


def _heatmap_query_context(dataset_id: int) -> dict[str, object]:
    metric = _metric("seek_event_count")
    form_data = _heatmap_form_data(dataset_id)
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [{"col": "event_date", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                "extras": {"time_grain_sqla": "P1D", "having": "", "where": ""},
                "applied_time_extras": {},
                "columns": [_table_column("position_bucket_30s"), "video_code"],
                "metrics": [metric],
                "orderby": [["position_bucket_30s", True], ["video_code", True]],
                "annotation_layers": [],
                "row_limit": 10000,
                "series_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": [],
            }
        ],
        "form_data": {**form_data, "force": False, "result_format": "json", "result_type": "full"},
        "result_format": "json",
        "result_type": "full",
    }


def _direction_form_data(dataset_id: int) -> dict[str, object]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "position_bucket_30s",
        "metrics": [
            _metric("seek_forward_count", label="forward seeks"),
            _metric("seek_backward_count", label="backward seeks"),
        ],
        "groupby": [],
        "adhoc_filters": _no_time_filter(),
        "row_limit": 10000,
        "order_desc": True,
        "orientation": "vertical",
        "comparison_type": "values",
        "annotation_layers": [],
        "stack": "Stack",
        "show_value": False,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_title": "30s position bucket",
        "y_axis_title": "seek events",
        "y_axis_format": "SMART_NUMBER",
        "truncateXAxis": True,
        "rich_tooltip": True,
        "showTooltipTotal": True,
        "extra_form_data": {},
        "dashboards": [],
    }


def _direction_query_context(dataset_id: int) -> dict[str, object]:
    metrics = [
        _metric("seek_forward_count", label="forward seeks"),
        _metric("seek_backward_count", label="backward seeks"),
    ]
    form_data = _direction_form_data(dataset_id)
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [{"col": "event_date", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                "extras": {"time_grain_sqla": "P1D", "having": "", "where": ""},
                "applied_time_extras": {},
                "columns": ["position_bucket_30s"],
                "metrics": metrics,
                "orderby": [["position_bucket_30s", True]],
                "annotation_layers": [],
                "row_limit": 10000,
                "series_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": [],
            }
        ],
        "form_data": {**form_data, "force": False, "result_format": "json", "result_type": "full"},
        "result_format": "json",
        "result_type": "full",
    }


def _summary_table_form_data(dataset_id: int) -> dict[str, object]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["video_code"],
        "metrics": [
            _metric("seek_count", label="total seek"),
            _metric("active_users", label="active users"),
            _metric("video_length_s", "MAX", label="video length s"),
            _metric("avg_watch_ratio", "AVG", label="avg watch ratio"),
            _metric("completion_rate", "AVG", label="completion rate"),
        ],
        "adhoc_filters": _no_time_filter(),
        "row_limit": 500,
        "order_desc": True,
        "server_pagination": False,
        "include_search": True,
        "show_cell_bars": False,
        "color_pn": False,
        "extra_form_data": {},
        "dashboards": [],
    }


def _summary_table_query_context(dataset_id: int) -> dict[str, object]:
    metrics = [
        _metric("seek_count", label="total seek"),
        _metric("active_users", label="active users"),
        _metric("video_length_s", "MAX", label="video length s"),
        _metric("avg_watch_ratio", "AVG", label="avg watch ratio"),
        _metric("completion_rate", "AVG", label="completion rate"),
    ]
    form_data = _summary_table_form_data(dataset_id)
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [{"col": "event_date", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                "extras": {"time_grain_sqla": "P1D", "having": "", "where": ""},
                "applied_time_extras": {},
                "columns": ["video_code"],
                "metrics": metrics,
                "orderby": [[metrics[0], False]],
                "annotation_layers": [],
                "row_limit": 500,
                "series_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": [],
            }
        ],
        "form_data": {**form_data, "force": False, "result_format": "json", "result_type": "full"},
        "result_format": "json",
        "result_type": "full",
    }


def _retention_form_data(dataset_id: int) -> dict[str, object]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "position_bucket_30s",
        "metrics": [_metric("retention_rate", "AVG", label="retention rate")],
        "groupby": [],
        "adhoc_filters": _no_time_filter(),
        "row_limit": 10000,
        "order_desc": False,
        "comparison_type": "values",
        "annotation_layers": [],
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_title": "30s position bucket",
        "y_axis_title": "retention rate",
        "y_axis_format": ".0%",
        "truncateXAxis": True,
        "rich_tooltip": True,
        "showTooltipTotal": False,
        "extra_form_data": {},
        "dashboards": [],
    }


def _retention_query_context(dataset_id: int) -> dict[str, object]:
    metrics = [_metric("retention_rate", "AVG", label="retention rate")]
    form_data = _retention_form_data(dataset_id)
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [{"col": "event_date", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                "extras": {"time_grain_sqla": "P1D", "having": "", "where": ""},
                "applied_time_extras": {},
                "columns": ["position_bucket_30s"],
                "metrics": metrics,
                "orderby": [["position_bucket_30s", True]],
                "annotation_layers": [],
                "row_limit": 10000,
                "series_limit": 0,
                "order_desc": False,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": [],
            }
        ],
        "form_data": {**form_data, "force": False, "result_format": "json", "result_type": "full"},
        "result_format": "json",
        "result_type": "full",
    }


def _build_position_json(charts: ChartRefs, dashboard_id: int) -> str:
    layout = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-1", "ROW-2", "ROW-3"],
            "meta": {},
        },
        "ROW-1": {
            "id": "ROW-1",
            "type": "ROW",
            "parents": ["GRID_ID"],
            "children": [f"CHART-{charts.heatmap}"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        f"CHART-{charts.heatmap}": {
            "id": f"CHART-{charts.heatmap}",
            "type": "CHART",
            "parents": ["ROW-1", "GRID_ID", "ROOT_ID"],
            "children": [],
            "meta": {
                "chartId": charts.heatmap,
                "height": 50,
                "width": 12,
                "sliceName": "Course seek map: video x position",
                "dashboardId": dashboard_id,
            },
        },
        "ROW-2": {
            "id": "ROW-2",
            "type": "ROW",
            "parents": ["GRID_ID"],
            "children": [f"CHART-{charts.direction}", f"CHART-{charts.retention}"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        f"CHART-{charts.direction}": {
            "id": f"CHART-{charts.direction}",
            "type": "CHART",
            "parents": ["ROW-2", "GRID_ID", "ROOT_ID"],
            "children": [],
            "meta": {
                "chartId": charts.direction,
                "height": 50,
                "width": 6,
                "sliceName": "Selected video: seek direction by position",
                "dashboardId": dashboard_id,
            },
        },
        f"CHART-{charts.retention}": {
            "id": f"CHART-{charts.retention}",
            "type": "CHART",
            "parents": ["ROW-2", "GRID_ID", "ROOT_ID"],
            "children": [],
            "meta": {
                "chartId": charts.retention,
                "height": 50,
                "width": 6,
                "sliceName": "Selected video: retention by position",
                "dashboardId": dashboard_id,
            },
        },
        "ROW-3": {
            "id": "ROW-3",
            "type": "ROW",
            "parents": ["GRID_ID"],
            "children": [f"CHART-{charts.summary_table}"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        f"CHART-{charts.summary_table}": {
            "id": f"CHART-{charts.summary_table}",
            "type": "CHART",
            "parents": ["ROW-3", "GRID_ID", "ROOT_ID"],
            "children": [],
            "meta": {
                "chartId": charts.summary_table,
                "height": 50,
                "width": 12,
                "sliceName": "Course video summary table",
                "dashboardId": dashboard_id,
            },
        },
    }
    return json.dumps(layout)


def _native_filter_targets(datasets: DatasetRefs, column_name: str) -> list[dict[str, object]]:
    return [
        {"datasetId": datasets.hotspots, "column": {"name": column_name}},
        {"datasetId": datasets.summary, "column": {"name": column_name}},
        {"datasetId": datasets.retention, "column": {"name": column_name}},
    ]


def _native_filter_config(
    *,
    datasets: DatasetRefs,
    charts: ChartRefs,
    course_filter_id: str,
    video_filter_id: str,
) -> list[dict[str, object]]:
    charts_in_scope = [charts.heatmap, charts.direction, charts.summary_table, charts.retention]
    return [
        {
            "id": course_filter_id,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Course",
            "filterType": "filter_select",
            "targets": _native_filter_targets(datasets, "course_id"),
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": charts_in_scope,
            "type": "NATIVE_FILTER",
            "description": "Chon 1 hoac nhieu course. De trong nghia la xem tat ca.",
        },
        {
            "id": video_filter_id,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Video",
            "filterType": "filter_select",
            "targets": _native_filter_targets(datasets, "video_code"),
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [course_filter_id],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": charts_in_scope,
            "type": "NATIVE_FILTER",
            "description": "Chon 1 hoac nhieu video. De trong nghia la xem tat ca.",
        },
    ]


def _build_dashboard_metadata(datasets: DatasetRefs, charts: ChartRefs, course_filter_id: str, video_filter_id: str) -> str:
    metadata = {
        "chart_configuration": {},
        "global_chart_configuration": {
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": [charts.heatmap, charts.direction, charts.summary_table, charts.retention],
        },
        "map_label_colors": {},
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "color_scheme": "",
        "label_colors": {},
        "shared_label_colors": [],
        "color_scheme_domain": [],
        "cross_filters_enabled": True,
        "filter_bar_orientation": "HORIZONTAL",
        "native_filter_configuration": _native_filter_config(
            datasets=datasets,
            charts=charts,
            course_filter_id=course_filter_id,
            video_filter_id=video_filter_id,
        ),
    }
    return json.dumps(metadata)


def find_database_id(client: SupersetClient, database_name: str) -> int:
    for database in client.list_objects("database"):
        if database.get("database_name") == database_name:
            return int(database["id"])
    raise SupersetApiError(f"Superset database {database_name!r} not found")


def ensure_dataset(client: SupersetClient, *, database_id: int, catalog: str, schema: str, table_name: str) -> int:
    for dataset in client.list_objects("dataset"):
        if (
            dataset.get("table_name") == table_name
            and dataset.get("schema") == schema
            and dataset.get("catalog") == catalog
        ):
            return int(dataset["id"])

    payload = {
        "database": database_id,
        "catalog": catalog,
        "schema": schema,
        "table_name": table_name,
        "owners": [1],
    }
    response = client.request("POST", "/api/v1/dataset/", payload)
    return int(response["id"])


def ensure_chart(
    client: SupersetClient,
    *,
    name: str,
    datasource_id: int,
    viz_type: str,
    params: dict[str, object],
    query_context: dict[str, object],
) -> int:
    existing_id: int | None = None
    for chart in client.list_objects("chart"):
        if chart.get("slice_name") == name:
            existing_id = int(chart["id"])
            break

    payload = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "query_context": json.dumps(query_context),
    }
    if existing_id is None:
        response = client.request("POST", "/api/v1/chart/", payload)
        return int(response["id"])

    client.request("PUT", f"/api/v1/chart/{existing_id}", payload)
    return existing_id


def ensure_dashboard(client: SupersetClient, *, title: str) -> int:
    for dashboard in client.list_objects("dashboard"):
        if dashboard.get("dashboard_title") == title:
            return int(dashboard["id"])

    payload = {
        "dashboard_title": title,
        "published": False,
        "json_metadata": json.dumps({"cross_filters_enabled": True}),
        "position_json": "{}",
        "owners": [1],
    }
    response = client.request("POST", "/api/v1/dashboard/", payload)
    return int(response["id"])


def delete_chart_if_exists(client: SupersetClient, *, name: str) -> None:
    for chart in client.list_objects("chart"):
        if chart.get("slice_name") == name:
            client.request("DELETE", f"/api/v1/chart/{int(chart['id'])}")
            return


def attach_chart_to_dashboard(
    client: SupersetClient,
    *,
    chart_id: int,
    dashboard_id: int,
    datasource_id: int,
    slice_name: str,
    viz_type: str,
) -> None:
    payload = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "dashboards": [dashboard_id],
    }
    client.request("PUT", f"/api/v1/chart/{chart_id}", payload)


def update_dashboard_layout(
    client: SupersetClient,
    *,
    dashboard_id: int,
    title: str,
    datasets: DatasetRefs,
    charts: ChartRefs,
) -> None:
    course_filter_id = f"NATIVE_FILTER-{secrets.token_urlsafe(8)}"
    video_filter_id = f"NATIVE_FILTER-{secrets.token_urlsafe(8)}"
    payload = {
        "dashboard_title": title,
        "published": False,
        "position_json": _build_position_json(charts, dashboard_id),
        "json_metadata": _build_dashboard_metadata(datasets, charts, course_filter_id, video_filter_id),
        "owners": [1],
    }
    client.request("PUT", f"/api/v1/dashboard/{dashboard_id}", payload)


def build_dashboard(
    *,
    dashboard_title: str,
    catalog: str,
    schema: str,
    hotspots_table: str,
    summary_table: str,
    retention_table: str,
) -> SupersetObjectIds:
    if not SUPERSET_PASSWORD:
        raise SupersetApiError("SUPERSET_ADMIN_PASSWORD is required")

    client = SupersetClient(SUPERSET_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD, SUPERSET_PROVIDER)
    database_id = find_database_id(client, SUPERSET_DATABASE_NAME)
    datasets = DatasetRefs(
        hotspots=ensure_dataset(client, database_id=database_id, catalog=catalog, schema=schema, table_name=hotspots_table),
        summary=ensure_dataset(client, database_id=database_id, catalog=catalog, schema=schema, table_name=summary_table),
        retention=ensure_dataset(client, database_id=database_id, catalog=catalog, schema=schema, table_name=retention_table),
    )

    charts = ChartRefs(
        heatmap=ensure_chart(
            client,
            name=f"{dashboard_title} :: Course seek map: video x position",
            datasource_id=datasets.hotspots,
            viz_type="heatmap_v2",
            params=_heatmap_form_data(datasets.hotspots),
            query_context=_heatmap_query_context(datasets.hotspots),
        ),
        direction=ensure_chart(
            client,
            name=f"{dashboard_title} :: Selected video: seek direction by position",
            datasource_id=datasets.hotspots,
            viz_type="echarts_timeseries_bar",
            params=_direction_form_data(datasets.hotspots),
            query_context=_direction_query_context(datasets.hotspots),
        ),
        summary_table=ensure_chart(
            client,
            name=f"{dashboard_title} :: Course video summary table",
            datasource_id=datasets.summary,
            viz_type="table",
            params=_summary_table_form_data(datasets.summary),
            query_context=_summary_table_query_context(datasets.summary),
        ),
        retention=ensure_chart(
            client,
            name=f"{dashboard_title} :: Selected video: retention by position",
            datasource_id=datasets.retention,
            viz_type="echarts_timeseries_line",
            params=_retention_form_data(datasets.retention),
            query_context=_retention_query_context(datasets.retention),
        ),
    )

    dashboard_id = ensure_dashboard(client, title=dashboard_title)
    attach_chart_to_dashboard(
        client,
        chart_id=charts.heatmap,
        dashboard_id=dashboard_id,
        datasource_id=datasets.hotspots,
        slice_name=f"{dashboard_title} :: Course seek map: video x position",
        viz_type="heatmap_v2",
    )
    attach_chart_to_dashboard(
        client,
        chart_id=charts.direction,
        dashboard_id=dashboard_id,
        datasource_id=datasets.hotspots,
        slice_name=f"{dashboard_title} :: Selected video: seek direction by position",
        viz_type="echarts_timeseries_bar",
    )
    attach_chart_to_dashboard(
        client,
        chart_id=charts.summary_table,
        dashboard_id=dashboard_id,
        datasource_id=datasets.summary,
        slice_name=f"{dashboard_title} :: Course video summary table",
        viz_type="table",
    )
    attach_chart_to_dashboard(
        client,
        chart_id=charts.retention,
        dashboard_id=dashboard_id,
        datasource_id=datasets.retention,
        slice_name=f"{dashboard_title} :: Selected video: retention by position",
        viz_type="echarts_timeseries_line",
    )
    delete_chart_if_exists(client, name=f"{dashboard_title} :: Seek hotspots by video and bucket")
    delete_chart_if_exists(client, name=f"{dashboard_title} :: Seek detail for selected videos")
    update_dashboard_layout(client, dashboard_id=dashboard_id, title=dashboard_title, datasets=datasets, charts=charts)
    return SupersetObjectIds(dashboard_id=dashboard_id, datasets=datasets, charts=charts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a Superset dashboard for video seek analysis")
    parser.add_argument("--dashboard-title", default="Video Seek Explorer", help="Dashboard title to create or update.")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Catalog for the datasets")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema for the datasets")
    parser.add_argument("--hotspots-table", default=DEFAULT_HOTSPOTS_TABLE, help="Seek hotspots dataset table")
    parser.add_argument("--summary-table", default=DEFAULT_SUMMARY_TABLE, help="Course summary dataset table")
    parser.add_argument("--retention-table", default=DEFAULT_RETENTION_TABLE, help="Retention dataset table")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ids = build_dashboard(
        dashboard_title=args.dashboard_title,
        catalog=args.catalog,
        schema=args.schema,
        hotspots_table=args.hotspots_table,
        summary_table=args.summary_table,
        retention_table=args.retention_table,
    )
    print(
        json.dumps(
            {
                "dashboard_id": ids.dashboard_id,
                "datasets": {
                    "hotspots": ids.datasets.hotspots,
                    "summary": ids.datasets.summary,
                    "retention": ids.datasets.retention,
                },
                "charts": {
                    "heatmap": ids.charts.heatmap,
                    "direction": ids.charts.direction,
                    "summary_table": ids.charts.summary_table,
                    "retention": ids.charts.retention,
                },
                "dashboard_url": f"{SUPERSET_URL}/superset/dashboard/{ids.dashboard_id}/",
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
