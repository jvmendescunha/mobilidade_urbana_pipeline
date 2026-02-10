# Databricks notebook source
# MAGIC %md
# MAGIC **Carregando dados da camada bronze**

# COMMAND ----------

# Carrega arquivos a serem tratados
df_mco_bronze = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/mco")
df_tempo_real_bronze = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/tempo_real")
df_linhas_onibus = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/linhas_onibus")
df_tipo_de_dia = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/tipo_dia")
df_cod_viagem = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/cod_viagem")
df_cod_interrupcao = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/cod_interrupcao")
df_empresas = spark.read.parquet("/Volumes/mobilidade_urbana/bronze/empresas")

# COMMAND ----------

# MAGIC %md
# MAGIC **tratamentos e ingestão da tabela silver.mco**

# COMMAND ----------

# DBTITLE 1,tratamentos e ingestão da tabela silver.mco
import sys
sys.path.append("/Workspace/Users/jvmendescunha@gmail.com/mobilidade_urbana_pipeline/src/utils")
from util_functions import normalize_column_names
from pyspark.sql import functions as F

# Corrigir nomes das colunas (remove espaços desnecessários)
df_mco_silver = df_mco_bronze.select(
    [F.col(c).alias(c.strip()) for c in df_mco_bronze.columns]
)

# Ajustar schema usando regex, pois uma assinalação usando RDD não funcionaria no Databricks Serveless
df_mco_silver = (
    df_mco_silver
    .withColumn("VIAGEM",F.regexp_replace(F.col("VIAGEM"), "-", "/"))
    .withColumn("SAIDA_TS",F.to_timestamp(F.concat_ws(" ", F.col("VIAGEM"), F.col("SAIDA")),"dd/MM/yyyy HH:mm"))
    .withColumn("CHEGADA_TS",F.to_timestamp(F.concat_ws(" ", F.col("VIAGEM"), F.col("CHEGADA")),"dd/MM/yyyy HH:mm"))
    .withColumn("VIAGEM",F.to_date(F.col("VIAGEM"), "dd/MM/yyyy"))
    .withColumn("DATA FECHAMENTO",F.to_timestamp(F.col("DATA FECHAMENTO"), "dd/MM/yyyy HH:mm"))
    .withColumn("CATRACA SAIDA", F.col("CATRACA SAIDA").cast("int"))
    .withColumn("CATRACA CHEGADA", F.col("CATRACA CHEGADA").cast("int"))
    .withColumn("TOTAL USUARIOS", F.col("TOTAL USUARIOS").cast("int"))
)

# Deduplicação de linhas
df_mco_silver = df_mco_silver.dropDuplicates(["VIAGEM", "LINHA", "SUBLINHA", "saida_ts", "chegada_ts"])

#normalizar nomes das colunas
df_mco_silver = normalize_column_names(df_mco_silver)

# Remove colunas com nome vazio ou apenas espaços
cols_validas = [c for c in df_mco_silver.columns if c and c.strip() != ""]
df_mco_silver = df_mco_silver.select(cols_validas)

# Salva na camada silver
df_mco_silver.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.mco")

# COMMAND ----------

# MAGIC %md
# MAGIC **tratamentos e ingestão da tabela silver.tempo_real**

# COMMAND ----------

# Tratamento de nomes de colunas seguinto o dicionário de dados da PBH
df_tempo_real_silver = df_tempo_real_bronze.select(
    [F.col(c).alias(c.strip()) for c in df_tempo_real_bronze.columns]
)
columns = {
'EV': "codigo_evento",
'HR': "horario",
'LT': "latitude",
'LG': "longitude",
'NV': "numero_ordem_veiculo",
'VL': "velocidade",
'NL': "nome_linha",
'DG': "direcao",
'SV': "sentido",
'DT': "distancia_percorrida"
}

for old_col, new_col in columns.items():
    if old_col in df_tempo_real_silver.columns:
        df_tempo_real_silver = df_tempo_real_silver.withColumnRenamed(old_col, new_col)


# Ajustar schema usando regex, pois uma assinalação usando RDD não funcionaria no Databricks Serveless
df_tempo_real_silver = (
    df_tempo_real_silver
    .withColumn("distancia_percorrida", F.col("distancia_percorrida").cast("double"))
    .withColumn("velocidade", F.col("velocidade").cast("int"))
    .withColumn("horario", F.to_timestamp(F.col("horario").cast("string"), "yyyyMMddHHmmss"))
)

# Deduplicação de linhas
df_tempo_real_silver = df_tempo_real_silver.dropDuplicates()

#salva na camada silver
df_tempo_real_silver.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.tempo_real")



# COMMAND ----------

# MAGIC %md
# MAGIC **tratamentos e ingestão de dicionários na camada silver**

# COMMAND ----------

# normalizar nomes das colunas
df_linhas_onibus = normalize_column_names(df_linhas_onibus)
df_tipo_de_dia = normalize_column_names(df_tipo_de_dia)
df_cod_viagem = normalize_column_names(df_cod_viagem)
df_cod_interrupcao = normalize_column_names(df_cod_interrupcao)
df_empresas = normalize_column_names(df_empresas)

# salvar dicionários na camada silver para facilitar queries
df_linhas_onibus.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.linhas_onibus")
df_tipo_de_dia.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.tipo_de_dia")
df_cod_viagem.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.cod_viagem")
df_cod_interrupcao.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.cod_interrupcao")
df_empresas.write.mode("overwrite").format("delta").saveAsTable("mobilidade_urbana.silver.empresas")

