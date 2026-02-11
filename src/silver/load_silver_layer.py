# Databricks notebook source
# MAGIC %md
# MAGIC **Carregando dados da camada bronze**

# COMMAND ----------

# Carrega arquivo da camada bronze
df_mco_bronze = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/mco")

# Cria schema silver, caso ainda não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS mobilidade_urbana.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **tratamentos e ingestão da tabela silver.mco**

# COMMAND ----------

# DBTITLE 1,tratamentos e ingestão da tabela silver.mco
from src.utils.util_functions import normalize_column_names
from pyspark.sql import functions as F

# Corrigir nomes das colunas (remove espaços desnecessários)
df_mco_silver = df_mco_bronze.select(
    [F.col(c).alias(c.strip()) for c in df_mco_bronze.columns]
)

# Remove colunas com nome vazio ou apenas espaços
cols_validas = [c for c in df_mco_silver.columns if c and c.strip() != ""]
df_mco_silver = df_mco_silver.select(cols_validas)

# Ajustar schema usando regex, pois uma assinalação usando RDD não funcionaria no Databricks Serveless
df_mco_silver = (
    df_mco_silver
    .withColumn("VIAGEM",F.regexp_replace(F.col("VIAGEM"), "-", "/"))
    .withColumn("SAIDA_TS",F.to_timestamp(F.concat_ws(" ", F.col("VIAGEM"), F.col("SAIDA")),"dd/MM/yyyy HH:mm"))
    .withColumn("CHEGADA_TS",F.to_timestamp(F.concat_ws(" ", F.col("VIAGEM"), F.col("CHEGADA")),"dd/MM/yyyy HH:mm"))
    .withColumn("VIAGEM",F.to_date(F.col("VIAGEM"), "dd/MM/yyyy"))
    .withColumn("CATRACA SAIDA", F.col("CATRACA SAIDA").cast("int"))
    .withColumn("CATRACA CHEGADA", F.col("CATRACA CHEGADA").cast("int"))
    .withColumn("TOTAL USUARIOS", F.col("TOTAL USUARIOS").cast("int"))
)

# Deduplicação de linhas
df_mco_silver = df_mco_silver.dropDuplicates(["VIAGEM", "LINHA", "SUBLINHA", "SAIDA_TS", "CHEGADA_TS"])

# #normalizar nomes das colunas
df_mco_silver = normalize_column_names(df_mco_silver)

# Salva na camada silver
df_mco_silver.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.mco")
