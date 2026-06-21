from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta_batch(
    df: DataFrame,
    output_path: str,
    *,
    mode: str = "append",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.format("delta").mode(mode).option("path", output_path)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(output_path)


def write_delta_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    query_name: str | None = None,
    partition_by: list[str] | None = None,
) -> None:
    writer = (
        df.writeStream.format("delta")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if query_name:
        writer = writer.queryName(query_name)
    writer.start().awaitTermination()
