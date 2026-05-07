from pyspark.sql import DataFrame


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
