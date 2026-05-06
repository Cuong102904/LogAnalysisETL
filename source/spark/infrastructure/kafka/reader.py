from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
    consumer_group_id: str | None = None,
    consumer_client_id: str | None = None,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
    )
    if consumer_group_id:
        reader = reader.option("kafka.group.id", consumer_group_id)
    if consumer_client_id:
        reader = reader.option("kafka.client.id", consumer_client_id)
    return reader.load()
