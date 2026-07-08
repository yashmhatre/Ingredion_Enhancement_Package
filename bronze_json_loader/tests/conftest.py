import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("bronze_json_loader-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
