from pyspark.sql import functions as F

# Cria schema gold, caso ainda não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS mobilidade_urbana.gold")

df_mco = spark.table("mobilidade_urbana.silver.mco")

# gold_viagens

df_gold_viagens = (
    df_mco
    .groupBy("viagem", "linha", "sublinha")
    .agg(
        F.count("viagem").alias("qtd_viagens"),
        F.countDistinct("veiculo").alias("qtd_veiculos"),
        F.sum("extensao").alias("distancia_percorrida"),
        F.sum(F.when(F.col("indicador_fechamento") == "F", 1).otherwise(0)).alias("qtd_fechadas"),
        F.sum("total_usuarios").alias("passageiros_transportados")
    )
    .withColumnRenamed("viagem", "data")
    .withColumn("faturamento_estimado", F.col("passageiros_transportados") * F.lit(6.25))
)

df_gold_viagens.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("mobilidade_urbana.gold.gold_viagens")

# gold_ocorrencias

df_gold_ocorrencias = (
    df_mco
    .filter(F.trim(F.col("ocorrencia")) != "")
    .groupBy("viagem", "linha", "ocorrencia", "justificativa")
    .agg(
        F.sum(F.when(F.col("ocorrencia") == "VI", 1).otherwise(0)).alias("viagens_interrupidas"),
        F.sum(F.when(F.col("ocorrencia") == "VN", 1).otherwise(0)).alias("viagens_nao_realizadas"),
        F.sum(F.when(F.col("falha_mecanica") == "S", 1).otherwise(0)).alias("falhas_mecanicas"),
        F.sum(F.when(F.col("evento_inseguro") == "S", 1).otherwise(0)).alias("eventos_inseguros"),
        F.sum("total_usuarios").alias("passageiros_afetados")
    )
    .withColumnRenamed("viagem", "data")
)

df_gold_ocorrencias.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("mobilidade_urbana.gold.gold_ocorrencias")

# gold_tipo_dia

df_gold_tipo_dia = (
    df_mco
    .groupBy("tipo_dia", "linha")
    .agg(
        F.count("viagem").alias("qtd_viagens"),
        F.countDistinct("veiculo").alias("qtd_veiculos"),
        F.sum("total_usuarios").alias("passageiros_transportados")
    )
    .withColumn("faturamento_estimado", F.col("passageiros_transportados") * F.lit(6.25))
)

df_gold_tipo_dia.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("mobilidade_urbana.gold.gold_tipo_dia")
