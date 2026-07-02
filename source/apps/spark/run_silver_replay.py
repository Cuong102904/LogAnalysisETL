from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from apps.spark.common import load_source_silver_plan
from learnlake.runtime import build_spark
from learnlake.silver.runtime import silver_output_paths, transform_bronze_batch, write_output_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay unresolved Silver unknown events.")
    parser.add_argument("--source", required=True)
    return parser.parse_args()


def _replay_input_df(unknown_df):
    return unknown_df.select(
        "event_id",
        F.col("raw_json").alias("raw_payload"),
        F.col("event_time_utc").alias("event_time"),
        F.col("first_seen_at").alias("ingestion_time"),
    )


def _drop_existing_canonical(outputs, output_paths, spark):
    canonical_key = "events_canonical"
    canonical = outputs.get(canonical_key)
    if canonical is None:
        return outputs
    try:
        existing = spark.read.format("delta").load(output_paths[canonical_key]).select("event_id")
        outputs[canonical_key] = canonical.join(existing, on="event_id", how="left_anti")
    except Exception:
        pass
    return outputs


def main() -> int:
    args = parse_args()
    spark = build_spark("learnlake_silver_unknown_replay")
    plan = load_source_silver_plan(args.source)
    output_paths = silver_output_paths(plan.profile)

    unknown_table = plan.profile.silver.unknown.table
    canonical_table = plan.profile.silver.canonical.table
    unknown_df = spark.read.format("delta").load(output_paths[unknown_table])
    unresolved = unknown_df.filter(F.col("replay_status") == "unresolved")

    if unresolved.rdd.isEmpty():
        print("learnlake silver replay no unresolved unknown events", flush=True)
        return 0

    outputs = transform_bronze_batch(_replay_input_df(unresolved), plan)
    outputs = _drop_existing_canonical(outputs, output_paths, spark)

    write_output_tables(
        {
            table: df
            for table, df in outputs.items()
            if table != unknown_table
        },
        output_paths,
    )

    still_unknown = outputs[unknown_table].select("event_id").dropDuplicates(["event_id"])
    resolved = (
        unresolved.join(still_unknown, on="event_id", how="left_anti")
        .join(
            outputs[canonical_table].select("event_id", "route_id"),
            on="event_id",
            how="left",
        )
        .withColumn("replay_status", F.lit("resolved"))
        .withColumn("last_replayed_at", F.current_timestamp())
        .withColumn("resolved_at", F.current_timestamp())
        .withColumnRenamed("route_id", "resolved_route_id")
    )
    unresolved_retry = (
        unresolved.join(still_unknown, on="event_id", how="inner")
        .withColumn("last_replayed_at", F.current_timestamp())
    )
    untouched = unknown_df.join(unresolved.select("event_id"), on="event_id", how="left_anti")
    updated_unknown = untouched.unionByName(unresolved_retry, allowMissingColumns=True).unionByName(
        resolved,
        allowMissingColumns=True,
    )
    updated_unknown.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_paths[unknown_table]
    )
    print("learnlake silver replay complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
