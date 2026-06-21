from __future__ import annotations

import re
from pathlib import Path


def test_core_package_does_not_contain_source_specific_identifiers() -> None:
    forbidden = [
        "daotao",
        "edx.",
        "edx_",
        "/courses/",
        "seq_next",
        "textbook.pdf",
        "special_exam",
        "problem.submitted",
        "hocbk",
        "soict",
        "course-v1:",
    ]
    root = Path("src/learnlake")
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for pattern in forbidden:
            if pattern in text:
                offenders.append(f"{path}: {pattern}")

    assert offenders == []


def test_legacy_top_level_technology_folders_are_removed() -> None:
    root = Path.cwd()
    forbidden = [
        "spark",
        "kafka",
        "airflow",
        "trino",
        "superset",
        "minio",
        "hive-metastore",
    ]

    offenders = [name for name in forbidden if (root / name).exists()]

    assert offenders == []


def test_supported_surfaces_do_not_reference_removed_top_level_paths() -> None:
    supported_files = [
        Path("README.md"),
        Path("docker-compose.yaml"),
        Path("docs/architecture.md"),
        Path("docs/event-flow.md"),
        Path("docs/local-setup.md"),
        Path("docs/repository-layout.md"),
        Path("docs/runbook.md"),
        Path("docs/schema-strategy.md"),
        Path("docs/trino.md"),
        Path("orchestration/airflow/Dockerfile"),
        Path("orchestration/airflow/config/variables.py"),
        Path("orchestration/airflow/tasks/spark_task.py"),
        Path("serving/trino/README.md"),
        Path("serving/trino/views/README.md"),
    ]
    forbidden_patterns = [
        r"\./spark\b",
        r"\./kafka\b",
        r"\./airflow\b",
        r"\./trino\b",
        r"\./superset\b",
        r"/opt/project/spark\b",
        r"(?<!apps/)spark/apps/",
        r"(?<!platform/local/)kafka/config/",
        r"(?<!serving/)trino/views/",
        r"(?<!serving/)superset/bootstrap/",
        r"(?<!orchestration/)airflow/dags/",
        r"(?<!orchestration/)airflow/tasks/",
    ]

    offenders = []
    for path in supported_files:
        text = path.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            if re.search(pattern, text):
                offenders.append(f"{path}: {pattern}")

    assert offenders == []
