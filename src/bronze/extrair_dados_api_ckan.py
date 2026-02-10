# Databricks notebook source
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

from src.utils.ckan_client import CKANClient
from src.config.settings import (
    CKAN_BASE_URL,
    CKAN_RESOURCES,
    BRONZE_DATASETS,
    CKAN_DEFAULT_PAGE_LIMIT
)

# -----------------------------------------------------
# CONFIGURAÇÃO DE LOG
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------------------------------
# MAIN
# -----------------------------------------------------
def main():
    logging.info("Iniciando ingestão BRONZE - Ônibus Tempo Real")

    spark = SparkSession.builder.appName(
        "bronze_ingest_onibus_tempo_real"
    ).getOrCreate()

    # Configurações do dataset
    dataset_config = CKAN_RESOURCES["onibus_tempo_real"]
    resource_id = dataset_config["resource_id"]
    output_path = BRONZE_DATASETS["onibus_tempo_real"]

    logging.info(f"Resource ID: {resource_id}")
    logging.info(f"Output path: {output_path}")

    # Cliente CKAN
    ckan_client = CKANClient(CKAN_BASE_URL)

    # Extração
    df = ckan_client.fetch_as_spark_df(
        spark=spark,
        resource_id=resource_id,
        limit=CKAN_DEFAULT_PAGE_LIMIT
    )

    if df.rdd.isEmpty():
        logging.warning("DataFrame vazio. Nada será gravado.")
        return

    # Metadados de ingestão
    ingestion_date = datetime.utcnow().date().isoformat()

    df_bronze = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(ingestion_date))
    )

    # Escrita BRONZE (imutável)
    (
        df_bronze
        .write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )

    logging.info(
        f"Ingestão finalizada com sucesso. "
        f"Registros gravados em {output_path}"
    )


if __name__ == "__main__":
    main()
