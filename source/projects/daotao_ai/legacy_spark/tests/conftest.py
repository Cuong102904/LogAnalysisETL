import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

SPARK_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_ROOT))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("spark-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()
