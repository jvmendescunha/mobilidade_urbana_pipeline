# Databricks notebook source
# MAGIC %md
# MAGIC **Leitura dos dados**

# COMMAND ----------

path_list_mco = [
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco01-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-02-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-03-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco04-06-2024-.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-07-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-08-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-09-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-10-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-11-2024.csv',
    '/Volumes/mobilidade_urbana/raw_data/csv_mco/mco-12-2024.csv',
]
path_tempo_real = '/Volumes/mobilidade_urbana/raw_data/csv_tempo_real/tempo_real_convencional_csv_090226064135.csv'

df_tempo_real = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(path_tempo_real)
)

df_mco = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(path_list_mco)
)


# COMMAND ----------

# MAGIC %md
# MAGIC **Ingestão dos dados em volumes na camada bronze**

# COMMAND ----------

from pyspark.sql import functions as F

bronze_schema_path = "/Volumes/mobilidade_urbana/bronze/"

df_tempo_real_bronze = df_tempo_real.withColumn("ingestion_timestamp", F.current_timestamp())
df_mco_bronze = df_mco.withColumn("ingestion_timestamp", F.current_timestamp())

df_tempo_real_bronze.write.mode("append").parquet(f"{bronze_schema_path}tempo_real")
df_mco_bronze.write.mode("append").parquet(f"{bronze_schema_path}mco")
