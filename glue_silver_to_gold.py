"""
Glue Job - Silver -> Gold
--------------------------
Lê os dados limpos da camada Silver e calcula agregados de negócio:
    - média mensal de cada série
    - variação percentual mês a mês (mom)
Grava o resultado em Parquet na camada Gold, pronto para consulta no Athena.

Job parameters esperados:
    --BUCKET_NAME     -> nome do bucket (ex: joao-medalhao-bcb-dados)
    --GLUE_DATABASE   -> database do Glue Data Catalog (mesmo da Silver)
    --GLUE_TABLE      -> tabela da Silver no catálogo (ex: silver)

IAM: a role do Glue Job precisa de:
    - s3:GetObject no prefixo silver/*
    - s3:PutObject no prefixo gold/*
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "BUCKET_NAME", "GLUE_DATABASE", "GLUE_TABLE"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME = args["BUCKET_NAME"]
GOLD_PATH = f"s3://{BUCKET_NAME}/gold/"

# 1. Lê a camada Silver a partir do Glue Data Catalog
dyf_silver = glueContext.create_dynamic_frame.from_catalog(
    database=args["GLUE_DATABASE"],
    table_name=args["GLUE_TABLE"],
)
df_silver = dyf_silver.toDF()

# 2. Cria a coluna ano_mes para agrupar por mês
df_com_mes = df_silver.withColumn(
    "ano_mes", F.date_format(F.col("data_referencia"), "yyyy-MM")
)

# 3. Agrega: média mensal por série
df_media_mensal = df_com_mes.groupBy("codigo_serie", "ano_mes").agg(
    F.avg("valor").alias("valor_medio"),
    F.min("valor").alias("valor_minimo"),
    F.max("valor").alias("valor_maximo"),
    F.count("valor").alias("qtd_observacoes"),
)

# 4. Calcula variação percentual mês a mês (usa a média do mês anterior)
janela = Window.partitionBy("codigo_serie").orderBy("ano_mes")
df_gold = df_media_mensal.withColumn(
    "valor_medio_mes_anterior", F.lag("valor_medio").over(janela)
).withColumn(
    "variacao_percentual_mom",
    F.round(
        (F.col("valor_medio") - F.col("valor_medio_mes_anterior"))
        / F.col("valor_medio_mes_anterior")
        * 100,
        2,
    ),
)

# 5. Grava em Parquet particionado por série
df_gold.write.mode("overwrite").partitionBy("codigo_serie").parquet(GOLD_PATH)

job.commit()
