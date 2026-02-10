# Databricks notebook source
# MAGIC %md
# MAGIC **Leitura dos dados**

# COMMAND ----------

# lista paths de todos os arquivos disponilizados na pbh, sendo dados de mobilidade urbana de janeiro a dezembro de 2024 e os dicionários de dados relacionados

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
path_dicionario_linhas = '/Volumes/mobilidade_urbana/raw_data/csv_tempo_real/bhtrans_bdlinha.csv'
path_tipo_de_dia = '/Volumes/mobilidade_urbana/raw_data/dicionarios/ANEXO_01_TIPO_DE_DIA.csv'
path_cod_interrupcao = '/Volumes/mobilidade_urbana/raw_data/dicionarios/ANEXO_02_CODIGO_INTERRUPCAO_VIAGEM.csv'
path_cod_viagem = '/Volumes/mobilidade_urbana/raw_data/dicionarios/ANEXO_03_CODIGO_VIAGEM.csv'
path_empresas = '/Volumes/mobilidade_urbana/raw_data/dicionarios/ANEXO_EMPRESAS_TRANSPORTE.csv'

df_tempo_real = (spark.read.option("header", "true").option("sep", ";").csv(path_tempo_real))
df_mco = (spark.read.option("header", "true").option("sep", ";").csv(path_list_mco))
df_dicionario_linhas = (spark.read.option("header", "true").option("sep", ";").csv(path_dicionario_linhas))
df_tipo_de_dia = (spark.read.option("header", "true").option("sep", ";").csv(path_tipo_de_dia))
df_cod_interrupcao = (spark.read.option("header", "true").option("sep", ";").csv(path_cod_interrupcao))
df_cod_viagem = (spark.read.option("header", "true").option("sep", ";").csv(path_cod_viagem))
df_empresas = (spark.read.option("header", "true").option("sep", ";").csv(path_empresas))


# COMMAND ----------

# MAGIC %md
# MAGIC **Ingestão dos dados em volumes na camada bronze**

# COMMAND ----------

from pyspark.sql import functions as F

bronze_schema_path = "/Volumes/mobilidade_urbana/bronze/"

# Adicionando data da ingestão para facilitar o controle de qualidade
df_tempo_real_bronze = df_tempo_real.withColumn("ingestion_timestamp", F.current_timestamp())
df_mco_bronze = df_mco.withColumn("ingestion_timestamp", F.current_timestamp())

# # Salvando dados no formato parquet usando o modo append para adicionar novos dados ao longo do tempo sem sobreescrever dados antigos
df_tempo_real_bronze.write.mode("append").parquet(f"{bronze_schema_path}tempo_real")
df_mco_bronze.write.mode("append").parquet(f"{bronze_schema_path}mco")

# Salvando dicionários no modo overwrite pois não há necessidade de manter versões antigas
df_dicionario_linhas.write.mode("overwrite").parquet(f"{bronze_schema_path}linhas_onibus")
df_tipo_de_dia.write.mode("overwrite").parquet(f"{bronze_schema_path}tipo_dia")
df_cod_interrupcao.write.mode("overwrite").parquet(f"{bronze_schema_path}cod_interrupcao")
df_cod_viagem.write.mode("overwrite").parquet(f"{bronze_schema_path}cod_viagem")
df_empresas.write.mode("overwrite").parquet(f"{bronze_schema_path}empresas")
