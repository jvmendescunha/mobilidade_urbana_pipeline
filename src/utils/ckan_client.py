# Databricks notebook source
import requests
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
import logging

# -----------------------------------------------------
# CONFIGURAÇÃO DE LOG
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------------------------------
# CLIENTE CKAN
# -----------------------------------------------------
class CKANClient:
    """
    Cliente para consumir dados da API CKAN.
    Compatível com portais como dados.pbh.gov.br
    """

    def __init__(self, base_url: str):
        """
        :param base_url: URL base da API CKAN
        Ex: https://dados.pbh.gov.br/api/3/action
        """
        self.base_url = base_url.rstrip("/")

    def fetch_all_records(self, resource_id: str, limit: int = 10000) -> List[Dict[str, Any]]:
        """
        Busca todos os registros de um resource_id usando paginação.

        :param resource_id: ID do recurso CKAN
        :param limit: número de registros por página
        :return: lista de registros (dict)
        """
        offset = 0
        all_records: List[Dict[str, Any]] = []

        logging.info(f"Iniciando download do resource_id={resource_id}")

        while True:
            params = {
                "resource_id": resource_id,
                "limit": limit,
                "offset": offset
            }

            response = requests.get(
                f"{self.base_url}/datastore_search",
                params=params,
                timeout=30
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"Erro HTTP {response.status_code} ao acessar CKAN"
                )

            payload = response.json()

            if not payload.get("success"):
                raise RuntimeError(
                    f"Erro retornado pela API CKAN: {payload}"
                )

            records = payload["result"]["records"]

            if not records:
                break

            all_records.extend(records)
            offset += limit

            logging.info(
                f"{len(records)} registros baixados "
                f"(total={len(all_records)})"
            )

        logging.info(
            f"Download finalizado. Total de registros: {len(all_records)}"
        )

        return all_records

    def fetch_as_spark_df(self, spark: SparkSession, resource_id: str, limit: int = 10000) -> DataFrame:
        """
        Retorna os dados do CKAN diretamente como Spark DataFrame.

        :param spark: SparkSession ativa
        :param resource_id: ID do recurso CKAN
        :param limit: número de registros por página
        :return: Spark DataFrame
        """
        records = self.fetch_all_records(resource_id, limit)

        if not records:
            logging.warning("Nenhum registro retornado pela API CKAN")
            return spark.createDataFrame([], schema=None)

        df = spark.createDataFrame(records)
        return df
