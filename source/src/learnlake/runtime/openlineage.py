from __future__ import annotations

import os

from typing import Any

OPENLINEAGE_LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"
OPENLINEAGE_SPARK_PACKAGE = "io.openlineage:openlineage-spark_2.12:1.50.0"
DEFAULT_NAMESPACE = "learnlake-local"
DEFAULT_TRANSPORT_TYPE = "http"
DEFAULT_TRANSPORT_URL = "http://marquez-api:5000"


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def configure_openlineage(
    builder: Any, *, namespace: str | None = None
) -> Any:
    """Apply the shared OpenLineage Spark settings used by LearnLake jobs."""

    builder = builder.config("spark.jars.packages", OPENLINEAGE_SPARK_PACKAGE)
    builder = builder.config("spark.extraListeners", OPENLINEAGE_LISTENER)
    builder = builder.config(
        "spark.openlineage.namespace",
        namespace or _env("OPENLINEAGE_NAMESPACE", default=DEFAULT_NAMESPACE),
    )
    builder = builder.config(
        "spark.openlineage.transport.type",
        _env("OPENLINEAGE_TRANSPORT_TYPE", default=DEFAULT_TRANSPORT_TYPE),
    )
    builder = builder.config(
        "spark.openlineage.transport.url",
        _env("OPENLINEAGE_TRANSPORT_URL", default=DEFAULT_TRANSPORT_URL),
    )
    endpoint = _env("OPENLINEAGE_TRANSPORT_ENDPOINT")
    if endpoint:
        builder = builder.config("spark.openlineage.transport.endpoint", endpoint)
    return builder
