from pyspark.sql import Column, functions as F


def parse_raw_event_time(raw_value_col: Column) -> Column:
    return F.expr(f"try_to_timestamp({F.get_json_object(raw_value_col, '$.time')._jc.toString()})")
