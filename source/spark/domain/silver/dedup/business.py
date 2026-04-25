from pyspark.sql import DataFrame, functions as F


def business_dedup_learning(df: DataFrame) -> DataFrame:
    keyed = df.withColumn("ts_rounded_500ms", (F.col("ts").cast("double") * 2).cast("long"))
    return keyed.dropDuplicates(["user_id_int", "event_type", "video_id", "ts_rounded_500ms"]).drop("ts_rounded_500ms")
