import os
import sys
from pathlib import Path

import pytest

FIXTURE = str(Path(__file__).parent / "fixtures" / "eco2mix_sample.csv")

# Spark workers must use the same interpreter as the driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from eco2mix.batch import spark_session

    s = spark_session("eco2mix-tests", "local[2]")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def raw(spark):
    from eco2mix.batch import read_raw

    return read_raw(spark, FIXTURE)


@pytest.fixture(scope="session")
def silver(raw):
    from eco2mix.batch import to_silver

    return to_silver(raw).cache()
