from typing import Protocol

from pyspark.sql import DataFrame


class SourcePort(Protocol):
    def read(self) -> DataFrame: ...
