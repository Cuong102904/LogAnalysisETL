from pyspark.sql import DataFrame, functions as F


def parse_status_columns(df: DataFrame) -> DataFrame:
    parsed = F.from_json(F.col("value_raw"), "map<string,string>")
    return (
        df.withColumn("parsed_map", parsed)
        .withColumn("parse_status", F.when(F.col("parsed_map").isNull(), F.lit("invalid_json")).otherwise(F.lit("ok")))
        .withColumn("parse_error", F.when(F.col("parsed_map").isNull(), F.lit("json_parse_failed")))
        .drop("parsed_map")
    )
