import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartilhada entre todos os testes."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("mobilidade_urbana_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
