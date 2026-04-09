from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read raw Kafka topic, clean basic fields, and route to exam/learning/noise topics.")
    p.add_argument("--bootstrap-servers", default="broker1:29092")
    p.add_argument("--input-topic", default="lms.raw.events")
    p.add_argument("--exam-topic", default="lms.exam.events")
    p.add_argument("--learning-topic", default="lms.learning.events")
    p.add_argument("--noise-topic", default="lms.noise.events")
    p.add_argument("--checkpoint", default="s3a://bronze/lsp/checkpoints/route_raw_to_topics")
    p.add_argument("--starting-offsets", default="latest", choices=("earliest", "latest"))
    return p.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_raw_stream(
    spark: SparkSession, bootstrap_servers: str, input_topic: str, starting_offsets: str
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def _lower_json(path: str) -> F.Column:
    return F.lower(F.coalesce(F.get_json_object(F.col("value_json"), path), F.lit("")))


def classify_and_route(
    raw: DataFrame, exam_topic: str, learning_topic: str, noise_topic: str
) -> DataFrame:
    df = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_json"),
        F.col("timestamp").alias("kafka_ts"),
    )

    event_type = _lower_json("$.record.event_type")
    name = _lower_json("$.record.name")
    event_source = _lower_json("$.record.event_source")
    path = _lower_json("$.record.context.path")
    page = _lower_json("$.record.page")
    referer = _lower_json("$.record.referer")
    course_id = _lower_json("$.record.context.course_id")
    username = _lower_json("$.record.username")
    agent = _lower_json("$.record.agent")

    joined = F.concat_ws(" ", event_type, name, path, page, referer, course_id)
    exam_hit = (
        event_type.startswith("edx.special_exam.timed.attempt.")
        | joined.contains("edx_proctoring")
        | joined.contains("proctored_exam")
        | (event_type.isin("problem_check", "edx.grades.problem.submitted") & (joined.contains("finalexam") | joined.contains("in_exam")))
    )
    learning_hit = (
        event_type.isin("play_video", "pause_video", "seek_video", "speed_change_video", "completion")
        | event_type.startswith("edx.ui.lms.sequence.")
        | event_type.isin("problem_check", "edx.grades.problem.submitted")
    )
    noise_hit = (
        joined.contains("wp-login")
        | joined.contains("wp-json")
        | joined.contains("xmlrpc")
        | joined.contains("/admin/")
        | joined.contains("/.env")
        | joined.contains("/.git/config")
        | joined.contains("/robots.txt")
        | ((event_source == "server") & (username == "") & agent.rlike("bot|spider|crawler|scanner|semrush|bingbot|googlebot|applebot"))
    )

    routed_topic = (
        F.when(exam_hit, F.lit(exam_topic))
        .when(noise_hit, F.lit(noise_topic))
        .when(learning_hit, F.lit(learning_topic))
        .otherwise(F.lit(learning_topic))
    )

    cleaned = df.withColumn("route_topic", routed_topic)
    return cleaned.select(
        F.col("route_topic").alias("topic"),
        F.col("kafka_key").cast("binary").alias("key"),
        F.col("value_json").cast("binary").alias("value"),
    )


def write_routed_stream(df: DataFrame, bootstrap_servers: str, checkpoint: str) -> None:
    (
        df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
        .awaitTermination()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark("lsp_route_raw_to_topics")
    raw = read_raw_stream(spark, args.bootstrap_servers, args.input_topic, args.starting_offsets)
    routed = classify_and_route(raw, args.exam_topic, args.learning_topic, args.noise_topic)
    write_routed_stream(routed, args.bootstrap_servers, args.checkpoint)


if __name__ == "__main__":
    main()

