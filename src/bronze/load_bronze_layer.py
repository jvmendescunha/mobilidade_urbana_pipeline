from pyspark.sql import functions as F

from src.utils.util_functions import read_csv

# Cria schema bronze, caso ainda não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS mobilidade_urbana.bronze")

# Ingestão de dados da camada raw
base_path = "/Volumes/mobilidade_urbana/raw_data"

paths = f"{base_path}/csv_mco/*.csv"

df = read_csv(paths, sep=";")

bronze_schema_path = "/Volumes/mobilidade_urbana/bronze/"

# Adicionando data da ingestão para facilitar o controle de qualidade
df_mco_bronze = df.withColumn("ingestion_timestamp", F.current_timestamp())

# Salvando dados no formato parquet, mantendo histórico com modo append
df_mco_bronze.write.mode("append").parquet(f"{bronze_schema_path}mco")
