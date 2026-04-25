from typing import Protocol

from pyspark.sql import DataFrame


class SinkPort(Protocol):
    def write(self, df: DataFrame) -> None:
        ...
