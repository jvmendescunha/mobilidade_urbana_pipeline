import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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