from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def business_dedup_learning(df: DataFrame) -> DataFrame:
    keyed = df.withColumn("time_rounded_500ms", (F.col("time").cast("double") * 2).cast("long"))
    return keyed.dropDuplicates(["user_id_int", "event_type", "video_id", "time_rounded_500ms"]).drop(
        "time_rounded_500ms"
    )
