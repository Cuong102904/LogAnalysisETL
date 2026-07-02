from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession

from apps.spark.common import load_source_silver_plan
from learnlake.runtime import build_spark
from learnlake.silver.runtime import silver_output_paths, transform_bronze_batch, write_output_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake Silver streaming normalization.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    return parser.parse_args()


def _wait_for_delta_table(spark: SparkSession, path: str, timeout_seconds: int = 180) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    first_commit_path = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log/00000000000000000000.json")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        fs = first_commit_path.getFileSystem(hadoop_conf)
        if fs.exists(first_commit_path):
            return
        time.sleep(2)
    raise TimeoutError(f"Delta table schema was not created before timeout: {path}")


def main() -> int:
    args = parse_args()
    spark = build_spark("learnlake_silver_normalization")
    plan = load_source_silver_plan(args.source)
    input_path = args.input or plan.profile.bronze.path
    output_paths = silver_output_paths(plan.profile)
    _wait_for_delta_table(spark, input_path)
    bronze_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", plan.profile.silver.runtime.max_files_per_trigger)
        .load(input_path)
    )

    def process_batch(batch_df, batch_id: int) -> None:
        outputs = transform_bronze_batch(batch_df, plan)
        write_output_tables(outputs, output_paths)
        print(
            f"learnlake silver batch_id={batch_id} completed outputs={','.join(sorted(outputs.keys()))}",
            flush=True,
        )

    (
        bronze_stream.writeStream.option("checkpointLocation", plan.profile.silver.checkpoint)
        .trigger(processingTime=plan.profile.silver.runtime.trigger_processing_time)
        .foreachBatch(process_batch)
        .queryName("learnlake_silver_normalization")
        .start()
        .awaitTermination()
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
