from pyspark.sql import DataFrame, functions as F


def add_classification(df: DataFrame) -> DataFrame:
    event_source = F.lower(F.get_json_object(F.col("value_raw"), "$.event_source"))
    event_type = F.lower(F.get_json_object(F.col("value_raw"), "$.event_type"))
    name = F.lower(F.get_json_object(F.col("value_raw"), "$.name"))
    path = F.lower(F.get_json_object(F.col("value_raw"), "$.context.path"))

    learning = (event_source == "browser") & (event_type.contains("video") | event_type.contains("seq"))
    performance = (event_source == "server") & (event_type == "edx.grades.problem.submitted")
    system = (
        event_type.contains("proctoring")
        | event_type.contains("heartbeat")
        | event_type.contains("auth")
        | event_type.contains("login")
        | event_type.contains("session")
        | name.contains("auth")
        | name.contains("login")
        | name.contains("session")
        | path.contains("/auth/")
        | path.contains("/login")
        | path.contains("/session")
    )

    return df.withColumn(
        "silver_class",
        F.when(learning, F.lit("learning"))
        .when(performance, F.lit("performance"))
        .when(system, F.lit("system"))
        .otherwise(F.lit("unknown")),
    )
