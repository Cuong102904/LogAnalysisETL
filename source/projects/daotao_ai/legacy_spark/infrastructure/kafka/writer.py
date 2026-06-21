from pyspark.sql import DataFrame


def write_kafka_stream(df: DataFrame, bootstrap_servers: str, checkpoint_location: str) -> None:
    (
        df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
        .awaitTermination()
    )
