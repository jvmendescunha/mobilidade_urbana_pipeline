import pytest
from pyspark.sql import functions as F


@pytest.fixture
def df_bronze(spark):
    """Simula dados da camada bronze."""
    return spark.createDataFrame(
        [
            ("SC01", "A", "15/03/2024", "08:00", "09:30", "12345", "45"),
            ("SC01", "A", "15/03/2024", "10:00", "11:30", "12400", "30"),
            ("SC02", None, "15/03/2024", "08:00", "09:00", None, "60"),
        ],
        ["LINHA", "SUBLINHA", "VIAGEM", "SAIDA", "CHEGADA", "CATRACA SAIDA", "TOTAL USUARIOS"],
    )


class TestBronzeQuality:

    def test_sem_linhas_vazias(self, df_bronze):
        assert df_bronze.count() > 0

    def test_coluna_linha_sem_nulos(self, df_bronze):
        nulos = df_bronze.filter(F.col("LINHA").isNull()).count()
        assert nulos == 0, f"Encontrados {nulos} nulos na coluna LINHA"

    def test_schema_esperado(self, df_bronze):
        colunas_obrigatorias = {"LINHA", "VIAGEM", "SAIDA", "CHEGADA"}
        colunas_presentes = set(df_bronze.columns)
        faltando = colunas_obrigatorias - colunas_presentes
        assert not faltando, f"Colunas faltando: {faltando}"


class TestSilverQuality:

    def test_datas_dentro_do_intervalo(self, spark):
        df = spark.createDataFrame(
            [("2024-01-01",), ("2024-12-31",), ("2024-06-15",)],
            ["viagem"],
        ).withColumn("viagem", F.to_date("viagem"))

        fora = df.filter(
            (F.col("viagem") < "2024-01-01") | (F.col("viagem") > "2024-12-31")
        ).count()
        assert fora == 0, f"{fora} registros com data fora de 2024"

    def test_total_usuarios_nao_negativo(self, spark):
        df = spark.createDataFrame([(45,), (0,), (30,)], ["total_usuarios"])
        negativos = df.filter(F.col("total_usuarios") < 0).count()
        assert negativos == 0


class TestGoldQuality:

    def test_faturamento_consistente_com_passageiros(self, spark):
        df = spark.createDataFrame(
            [(75, 468.75), (0, 0.0), (100, 625.0)],
            ["passageiros_transportados", "faturamento_estimado"],
        )
        inconsistentes = df.filter(
            F.col("faturamento_estimado") != F.col("passageiros_transportados") * 6.25
        ).count()
        assert inconsistentes == 0

    def test_sem_duplicatas_na_chave(self, spark):
        df = spark.createDataFrame(
            [("2024-03-15", "SC01", "A"), ("2024-03-15", "SC02", "A"), ("2024-03-16", "SC01", "A")],
            ["data", "linha", "sublinha"],
        )
        total = df.count()
        distintos = df.dropDuplicates(["data", "linha", "sublinha"]).count()
        assert total == distintos, f"{total - distintos} duplicatas encontradas"