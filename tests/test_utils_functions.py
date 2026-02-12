import pytest
from pyspark.sql import functions as F
import re


def normalize_column_names(df):
    for col in df.columns:
        normalized = col.lower().strip().replace(" ", "_")
        normalized = re.sub(r"[^a-z0-9_]", "_", normalized)
        normalized = re.sub(r"_+", "_", normalized)
        normalized = normalized.strip("_")
        df = df.withColumnRenamed(col, normalized)
    return df


class TestNormalizeColumnNames:

    def test_espacos_viram_underscores(self, spark):
        df = spark.createDataFrame([(1,)], ["CATRACA SAIDA"])
        result = normalize_column_names(df)
        assert result.columns == ["catraca_saida"]

    def test_caracteres_especiais_removidos(self, spark):
        df = spark.createDataFrame([(1,)], ["DATA (FECHAMENTO)"])
        result = normalize_column_names(df)
        assert result.columns == ["data_fechamento"]

    def test_underscores_consecutivos_colapsados(self, spark):
        df = spark.createDataFrame([(1,)], ["FOO   BAR"])
        result = normalize_column_names(df)
        assert result.columns == ["foo_bar"]

    def test_underscores_inicio_e_fim_removidos(self, spark):
        df = spark.createDataFrame([(1,)], ["_LINHA_"])
        result = normalize_column_names(df)
        assert result.columns == ["linha"]

    def test_multiplas_colunas(self, spark):
        df = spark.createDataFrame(
            [(1, 2, 3)],
            ["TOTAL USUARIOS", "Linha#Bus", "OK"],
        )
        result = normalize_column_names(df)
        assert result.columns == ["total_usuarios", "linha_bus", "ok"]

    def test_coluna_ja_normalizada_nao_muda(self, spark):
        df = spark.createDataFrame([(1,)], ["linha"])
        result = normalize_column_names(df)
        assert result.columns == ["linha"]

    def test_acentos_substituidos(self, spark):
        df = spark.createDataFrame([(1,)], ["EXTENSÃO-KM"])
        result = normalize_column_names(df)
        assert result.columns == ["extens_o_km"]