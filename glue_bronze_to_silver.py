"""
Glue Job - Bronze -> Silver
----------------------------
Lê os dados brutos da camada Bronze (catalogados pelo Glue Crawler),
achata a estrutura aninhada (lista "dados" dentro de cada registro),
limpa/tipa os campos e grava em Parquet particionado por série na
camada Silver.

Job parameters esperados (configure em "Job details" -> "Job parameters"):
    --BUCKET_NAME     -> nome do bucket (ex: joao-medalhao-bcb-dados)
    --GLUE_DATABASE   -> nome do database do Glue Data Catalog
    --GLUE_TABLE      -> nome da tabela criada pelo Crawler sobre o bronze/

IAM: a role do Glue Job precisa de:
    - s3:GetObject no prefixo bronze/*
    - s3:PutObject no prefixo silver/*
    - glue:GetTable, glue:GetDatabase (para ler o catálogo)
"""

import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "BUCKET_NAME", "GLUE_DATABASE", "GLUE_TABLE"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME = args["BUCKET_NAME"]
SILVER_PATH = f"s3://{BUCKET_NAME}/silver/"

# 1. Lê a camada Bronze a partir do Glue Data Catalog (populado pelo Crawler)
dyf_bronze = glueContext.create_dynamic_frame.from_catalog(
    database=args["GLUE_DATABASE"],
    table_name=args["GLUE_TABLE"],
)
df_bronze = dyf_bronze.toDF()

# 2. Cada registro tem um array "dados" com pontos {data, valor} em string.
#    Explode o array para ter uma linha por ponto da série.
df_explodido = df_bronze.select(
    "codigo_serie",
    "fonte",
    "data_execucao",
    F.explode("dados").alias("ponto"),
)

# 3. Extrai e tipa os campos: data (string dd/mm/yyyy -> date) e valor (string -> double)
df_silver = (
    df_explodido.select(
        F.col("codigo_serie").cast("string").alias("codigo_serie"),
        F.col("fonte"),
        F.to_date(F.col("ponto.data"), "dd/MM/yyyy").alias("data_referencia"),
        F.col("ponto.valor").cast(DoubleType()).alias("valor"),
        F.col("data_execucao"),
    )
    # 4. Remove duplicatas (mesma série + mesma data, pode ocorrer entre execuções
    #    consecutivas que se sobrepõem na janela de 90 dias)
    .dropDuplicates(["codigo_serie", "data_referencia"])
    # 5. Descarta linhas sem valor ou sem data válida (ruído/erro de parsing)
    .filter(F.col("valor").isNotNull() & F.col("data_referencia").isNotNull())
)

# 6. Grava em Parquet particionado por série (bom para consultas seletivas no Athena)
df_silver.write.mode("overwrite").partitionBy("codigo_serie").parquet(SILVER_PATH)

job.commit()
