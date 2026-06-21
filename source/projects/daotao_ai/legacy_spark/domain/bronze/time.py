from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_raw_event_time(raw_value_col: Column) -> Column:
    raw_value_str = raw_value_col.cast("string")
    return F.try_to_timestamp(F.get_json_object(raw_value_str, "$.time"))
