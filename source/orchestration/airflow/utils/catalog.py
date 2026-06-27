from __future__ import annotations

import os
from functools import lru_cache

from learnlake.runtime import load_source_profile

SOURCE_ID = os.getenv("LEARNLAKE_SOURCE_ID", "daotao_ai")
GOLD_TABLE_BASE_PATH = "s3a://lakehouse/learnlake/gold"


@lru_cache(maxsize=1)
def source_profile():
    return load_source_profile(SOURCE_ID)


def bronze_table_path() -> str:
    return source_profile().bronze.path


def bronze_checkpoint_path() -> str:
    checkpoint = source_profile().bronze.checkpoint
    if not checkpoint:
        raise ValueError("bronze.checkpoint is required in the source catalog")
    return checkpoint


def silver_table_path(target_name: str) -> str:
    silver = source_profile().silver
    if target_name == "event_index":
        return silver.event_index.path
    if target_name == "invalid":
        if silver.invalid is None:
            raise ValueError("silver.invalid is not defined in the source catalog")
        return silver.invalid.path
    target = silver.targets[target_name]
    return target.path


def kafka_input_topic() -> str:
    topic = source_profile().input.topic
    if not topic:
        raise ValueError("input.topic is required in the source catalog")
    return topic


def parse_s3a_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3a://"):
        raise ValueError(f"Expected s3a URI, got: {uri}")
    bucket_and_key = uri.removeprefix("s3a://")
    bucket, _, key = bucket_and_key.partition("/")
    if not bucket:
        raise ValueError(f"Invalid s3a URI, missing bucket: {uri}")
    prefix = key.strip("/")
    if prefix:
        prefix = f"{prefix}/"
    return bucket, prefix
