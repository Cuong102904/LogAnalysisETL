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
DEFAULT_TABLE = "exam_question_difficulty_view"


class SupersetApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class DashboardRefs:
    dataset_id: int
    chart_id: int
    dashboard_id: int


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str, provider: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.provider = provider
        self.access_token = self._login()

    def _login(self) -> str:
        response = self.request(
            "POST",
            "/api/v1/security/login",
            {
                "username": self.username,
                "password": self.password,
                "provider": self.provider,
                "refresh": True,
            },
            include_auth=False,
        )
        return str(response["access_token"])

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

        request = urllib.request.Request(f"{self.base_url}{path}", data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                text = response.read().decode("utf-8")
                return json.loads(text) if text else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SupersetApiError(f"{method} {path} failed with HTTP {exc.code}: {detail}") from exc

    def list_objects(self, resource: str, page_size: int = 100) -> list[dict[str, object]]:
        query = urllib.parse.quote(f"(page:0,page_size:{page_size})", safe="(),:")
        response = self.request("GET", f"/api/v1/{resource}/?q={query}")
        return list(response.get("result", []))


def _filter_clause(subject: str, comparator: object, operator: str) -> dict[str, object]:
    return {
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


def _metric(column_name: str, aggregate: str = "AVG", *, label: str | None = None) -> dict[str, object]:
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


def _no_time_filter() -> list[dict[str, object]]:
    return [_filter_clause("event_date", "No filter", "TEMPORAL_RANGE")]


def _bar_form_data(dataset_id: int) -> dict[str, object]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "dist_bar",
        "query_mode": "aggregate",
        "groupby": ["question_key"],
        "metrics": [_metric("final_grade_ratio", "AVG", label="avg final grade ratio")],
        "adhoc_filters": _no_time_filter(),
        "row_limit": 20,
        "order_desc": False,
        "sort_by_metric": True,
        "show_legend": False,
        "orientation": "horizontal",
        "x_axis_title": "average final grade ratio",
        "y_axis_title": "question",
        "x_axis_format": ".0%",
        "extra_form_data": {},
        "dashboards": [],
    }


def _bar_query_context(dataset_id: int) -> dict[str, object]:
    metric = _metric("final_grade_ratio", "AVG", label="avg final grade ratio")
    form_data = _bar_form_data(dataset_id)
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [{"col": "event_date", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                "extras": {"having": "", "where": ""},
                "applied_time_extras": {},
                "columns": ["question_key"],
                "metrics": [metric],
                "orderby": [[metric, True]],
                "annotation_layers": [],
                "row_limit": 20,
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


def _build_position_json(chart_id: int, dashboard_id: int) -> str:
    layout = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-1"],
            "meta": {},
        },
        "ROW-1": {
            "id": "ROW-1",
            "type": "ROW",
            "parents": ["GRID_ID"],
            "children": [f"CHART-{chart_id}"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        f"CHART-{chart_id}": {
            "id": f"CHART-{chart_id}",
            "type": "CHART",
            "parents": ["ROW-1", "GRID_ID", "ROOT_ID"],
            "children": [],
            "meta": {
                "chartId": chart_id,
                "height": 60,
                "width": 12,
                "sliceName": "Hardest questions by average final grade ratio",
                "dashboardId": dashboard_id,
            },
        },
    }
    return json.dumps(layout)


def _build_dashboard_metadata(dataset_id: int, chart_id: int, course_filter_id: str, exam_filter_id: str) -> str:
    return json.dumps(
        {
            "chart_configuration": {},
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": [chart_id],
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
            "native_filter_configuration": [
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
                    "targets": [{"datasetId": dataset_id, "column": {"name": "course_id"}}],
                    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "chartsInScope": [chart_id],
                    "type": "NATIVE_FILTER",
                    "description": "Filter by one or more courses.",
                },
                {
                    "id": exam_filter_id,
                    "controlValues": {
                        "enableEmptyFilter": True,
                        "defaultToFirstItem": False,
                        "multiSelect": True,
                        "searchAllOptions": True,
                        "inverseSelection": False,
                    },
                    "name": "Exam",
                    "filterType": "filter_select",
                    "targets": [{"datasetId": dataset_id, "column": {"name": "exam_name"}}],
                    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                    "cascadeParentIds": [course_filter_id],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "chartsInScope": [chart_id],
                    "type": "NATIVE_FILTER",
                    "description": "Filter by exam title within the selected course set.",
                },
            ],
        }
    )


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

    response = client.request(
        "POST",
        "/api/v1/dataset/",
        {
            "database": database_id,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "owners": [1],
        },
    )
    return int(response["id"])


def ensure_chart(
    client: SupersetClient,
    *,
    name: str,
    datasource_id: int,
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
        "viz_type": "dist_bar",
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

    response = client.request(
        "POST",
        "/api/v1/dashboard/",
        {
            "dashboard_title": title,
            "published": False,
            "json_metadata": json.dumps({"cross_filters_enabled": True}),
            "position_json": "{}",
            "owners": [1],
        },
    )
    return int(response["id"])


def attach_chart_to_dashboard(client: SupersetClient, *, chart_id: int, dashboard_id: int, datasource_id: int, chart_name: str) -> None:
    client.request(
        "PUT",
        f"/api/v1/chart/{chart_id}",
        {
            "slice_name": chart_name,
            "viz_type": "dist_bar",
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "dashboards": [dashboard_id],
        },
    )


def update_dashboard_layout(client: SupersetClient, *, dashboard_id: int, title: str, dataset_id: int, chart_id: int) -> None:
    course_filter_id = f"NATIVE_FILTER-{secrets.token_urlsafe(8)}"
    exam_filter_id = f"NATIVE_FILTER-{secrets.token_urlsafe(8)}"
    client.request(
        "PUT",
        f"/api/v1/dashboard/{dashboard_id}",
        {
            "dashboard_title": title,
            "published": False,
            "position_json": _build_position_json(chart_id, dashboard_id),
            "json_metadata": _build_dashboard_metadata(dataset_id, chart_id, course_filter_id, exam_filter_id),
            "owners": [1],
        },
    )


def build_dashboard(*, dashboard_title: str, catalog: str, schema: str, table_name: str) -> DashboardRefs:
    if not SUPERSET_PASSWORD:
        raise SupersetApiError("SUPERSET_ADMIN_PASSWORD is required")

    client = SupersetClient(SUPERSET_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD, SUPERSET_PROVIDER)
    database_id = find_database_id(client, SUPERSET_DATABASE_NAME)
    dataset_id = ensure_dataset(client, database_id=database_id, catalog=catalog, schema=schema, table_name=table_name)
    chart_name = f"{dashboard_title} :: Hardest questions by average final grade ratio"
    chart_id = ensure_chart(
        client,
        name=chart_name,
        datasource_id=dataset_id,
        params=_bar_form_data(dataset_id),
        query_context=_bar_query_context(dataset_id),
    )
    dashboard_id = ensure_dashboard(client, title=dashboard_title)
    attach_chart_to_dashboard(
        client,
        chart_id=chart_id,
        dashboard_id=dashboard_id,
        datasource_id=dataset_id,
        chart_name=chart_name,
    )
    update_dashboard_layout(client, dashboard_id=dashboard_id, title=dashboard_title, dataset_id=dataset_id, chart_id=chart_id)
    return DashboardRefs(dataset_id=dataset_id, chart_id=chart_id, dashboard_id=dashboard_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a Superset dashboard for exam question difficulty analysis")
    parser.add_argument("--dashboard-title", default="Exam Question Difficulty Explorer", help="Dashboard title to create or update.")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Catalog for the dataset")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema for the dataset")
    parser.add_argument("--table-name", default=DEFAULT_TABLE, help="Dataset table or view name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    refs = build_dashboard(
        dashboard_title=args.dashboard_title,
        catalog=args.catalog,
        schema=args.schema,
        table_name=args.table_name,
    )
    print(
        json.dumps(
            {
                "dashboard_id": refs.dashboard_id,
                "dataset_id": refs.dataset_id,
                "chart_id": refs.chart_id,
                "dashboard_url": f"{SUPERSET_URL}/superset/dashboard/{refs.dashboard_id}/",
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
