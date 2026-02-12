from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder \
    .appName("Replicar Tabela CSV") \
    .getOrCreate()

schema = StructType([
    StructField("TIPO_DE_DIA", IntegerType(), True),
    StructField("DESCRICAO", StringType(), True)
])

data = [
    (1, "DOMINGO E FERIADO"),
    (2, "SEGUNDA-FEIRA (LINHA 535)"),
    (3, "TERÇA-FEIRA (LINHA 535)"),
    (4, "QUARTA-FEIRA (LINHA 535)"),
    (5, "QUINTA-FEIRA (LINHA 535)"),
    (6, "SEXTA-FEIRA (LINHA 535)"),
    (7, "SÁBADO"),
    (8, "DIA ÚTIL"),
    (9, "DIA ÚTIL-ATÍPICO"),
    (10, "DIA ÚTIL-FÉRIAS DE JULHO"),
    (11, "DOMINGO"),
    (12, "FERIADO"),
    (13, "SEGUNDA-FEIRA DE CARNAVAL"),
    (14, "DIA ÚTIL-ATÍPICO ESPECIAL"),
    (15, "FERIADO/DIA ÚTIL"),
    (16, "SÃO LÁZARO"),
    (17, "PAIXÃO"),
    (18, "CARNABELO SEXTA-FEIRA"),
    (19, "CARNABELO SÁBADO"),
    (20, "CARNABELO DOMINGO"),
    (21, "CARNABELO SEGUNDA-FEIRA"),
    (22, "SÁBADO-ATÍPICO"),
    (23, "ATENDIMENTO AO JOCKEY CLUB"),
    (24, "QUARTA-FEIRA DE CINZAS"),
    (25, "SETE DE SETEMBRO"),
    (26, "SEXTA APÓS FERIADO"),
    (27, "DIA ÚTIL - TERCEIRO DECÊNDIO"),
    (28, "DIA ÚTIL - SEGUNDO DECÊNDIO"),
    (29, "COPA DO MUNDO - JOGO DO BRASIL"),
    (30, "SÁBADO DE CARNAVAL"),
    (31, "DOMINGO/TERÇA DE CARNAVAL"),
    (32, "DIA ÚTIL-COVID-19"),
    (33, "SÁBADO-COVID-19"),
    (34, "DOMINGO/FERIADO-COVID-19")
]

df_tipo_dia = spark.createDataFrame(data, schema)
df_tipo_dia.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("mobilidade_urbana.gold.dim_tipo_dia")

schema_ocorrencia = StructType([
    StructField("CODIGO_EVENTO", StringType(), True),
    StructField("CODIGO_VIAGEM", StringType(), True),
    StructField("DESCRICAO", StringType(), True)
])

data_ocorrencia = [
    ("ODF", "VN", "ÔNIBUS COM DEFEITO"),
    ("STP", "VN", "SEM TRIPULAÇÃO"),
    ("APV", "VN", "ATRASO DE PERCURSO DE VIAGEM"),
    ("OUT", "VN", "OUTROS"),
    ("EAR", "VI", "ENTRADA DE AR"),
    ("EMB", "VI", "EMBREAGEM"),
    ("FRE", "VI", "FREIO"),
    ("MOT", "VI", "MOTOR"),
    ("ROD", "VI", "RODA"),
    ("POR", "VI", "PORTA"),
    ("PEL", "VI", "PARTE ELÉTRICA"),
    ("OUM", "VI", "OUTROS PROBLEMAS MECÂNICOS"),
    ("ATR", "VI", "ATROPELAMENTO"),
    ("ALB", "VI", "ABALROAMENTO"),
    ("OPE", "VI", "OUTROS ACIDENTES DE PERCURSO"),
    ("PNE", "VI", "PNEU FURADO"),
    ("DFP", "VI", "DEFEITO FREIO DE PORTA"),
    ("DRT", "VI", "DEFEITO ROLETE")
]

df_ocorrencia = spark.createDataFrame(data_ocorrencia, schema_ocorrencia)
df_ocorrencia.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("mobilidade_urbana.gold.dim_ocorrencia")
