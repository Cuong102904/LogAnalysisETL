from pyspark.sql import DataFrame


def window_dedup(
    df: DataFrame, ts_col: str, keys: list[str], watermark: str = "10 minutes"
) -> DataFrame:
    return df.withWatermark(ts_col, watermark).dropDuplicates(keys)
