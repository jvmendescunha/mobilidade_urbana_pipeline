import re
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pathlib import Path

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normaliza nome de colunas Spark para snake case
    """
    for col in df.columns:
        normalized = (
            col.lower()
            .strip()
            .replace(" ", "_")
        )
        normalized = re.sub(r"[^a-z0-9_]", "_", normalized)
        normalized = re.sub(r"_+", "_", normalized)
        normalized = normalized.strip("_")

        df = df.withColumnRenamed(col, normalized)

    return df

def read_csv(path, sep=";", header=True):
    return (
        spark.read
        .option("header", str(header).lower())
        .option("sep", sep)
        .csv(path)
    )