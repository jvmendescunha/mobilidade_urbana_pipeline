# Databricks notebook source
# MAGIC %md
# MAGIC **Leitura dos dados**

# COMMAND ----------

# DBTITLE 1,Untitled
# importa funções necessárias
from src.utils.util_functions import read_csv
from pyspark.sql import functions as F

# Cria schema bronze, caso ainda não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS mobilidade_urbana.bronze")

# lista paths de todos os arquivos disponilizados na pbh, sendo dados de mobilidade urbana de janeiro a dezembro de 2024 e os dicionários de dados relacionados
base_path = "/Volumes/mobilidade_urbana/raw_data"

paths = f"{base_path}/csv_mco/*.csv"

df = read_csv(paths, sep=";")

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingestão dos dados em volumes na camada bronze**

# COMMAND ----------

bronze_schema_path = "/Volumes/mobilidade_urbana/bronze/"

# Adicionando data da ingestão para facilitar o controle de qualidade
df_mco_bronze = df.withColumn("ingestion_timestamp", F.current_timestamp())

# Salvando dados no formato parquet usando o modo append para adicionar novos dados ao longo do tempo sem sobreescrever dados antigos
df_mco_bronze.write.mode("append").parquet(f"{bronze_schema_path}mco")

