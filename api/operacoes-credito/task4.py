import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = 'banco-central/operacoes-credito/df.parquet'
DEST_BUCKET_NAME = os.getenv("BUCKET_REFINED_NAME")
DEST_PATH = 'banco-central/operacoes-credito/df.parquet'

def lambda_handler(event, context):
    print("1")
    spark = SparkSession.builder.appName("ProcessamentoPySpark").getOrCreate()
    print("2")

    src_path_s3 = f"s3a://{SRC_BUCKET_NAME}/{SRC_PATH}"
    dest_path_s3 = f"s3a://{DEST_BUCKET_NAME}/{DEST_PATH}"
    print("3")

    df = spark.read.parquet(src_path_s3)
    print("4")

    df = df.withColumn(
        "vl_carteira_problematica",
        (col("vl_ativo_problematico") - col("vl_carteira_inadimplida_arrastada"))
    )
    print("5")

    df = df.withColumn(
        "vl_carteira_problematica",
        when(col("vl_carteira_problematica") < 0, 0).otherwise(col("vl_carteira_problematica"))
    )
    print("6")

    df = df.withColumn(
        "vl_carteira_ativa",
        col("vl_carteira_ativa") - col("vl_carteira_problematica")
    ).withColumnRenamed("vl_carteira_ativa", "vl_carteira_saudavel")
    print("7")

    df = df.withColumn(
        "vl_carteira_saudavel",
        col("vl_carteira_saudavel") / col("qt_numero_de_operacoes")
    ).withColumn(
        "vl_carteira_problematica",
        col("vl_carteira_problematica") / col("qt_numero_de_operacoes")
    ).withColumn(
        "vl_carteira_inadimplida_arrastada",
        col("vl_carteira_inadimplida_arrastada") / col("qt_numero_de_operacoes")
    )
    print("8")

    df = df.drop("qt_numero_de_operacoes")
    print("9")

    df.write.mode("overwrite").parquet(dest_path_s3)
    print("10")

    spark.stop()
    print("11")

def handler():
    return lambda_handler({}, {})
