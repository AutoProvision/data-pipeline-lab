import boto3
import os
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

s3_client = boto3.client('s3')

SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = 'banco-central/operacoes-credito/df.parquet'
DEST_BUCKET_NAME = os.getenv("BUCKET_REFINED_NAME")
DEST_PATH = 'banco-central/operacoes-credito/df.parquet'

def lambda_handler(event, context):
    print("1")
    spark = SparkSession.builder.appName("ProcessamentoS3").getOrCreate()
    print("2")

    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    print("3")
    df_bytes = BytesIO(obj['Body'].read())
    print("4")
    del obj

    df = spark.read.parquet(df_bytes)
    print("5")

    df = df.withColumn(
        "vl_carteira_problematica",
        (col("vl_ativo_problematico") - col("vl_carteira_inadimplida_arrastada")).cast("double")
    )
    print("6")

    df = df.withColumn(
        "vl_carteira_problematica",
        when(col("vl_carteira_problematica") < 0, 0).otherwise(col("vl_carteira_problematica"))
    )
    print("7")

    df = df.withColumn(
        "vl_carteira_saudavel",
        (col("vl_carteira_ativa") - col("vl_carteira_problematica")).cast("double")
    ).drop("vl_carteira_ativa")
    print("8")

    for col_name in ["vl_carteira_saudavel", "vl_carteira_problematica", "vl_carteira_inadimplida_arrastada"]:
        df = df.withColumn(col_name, col(col_name) / col("qt_numero_de_operacoes"))
    print("9")

    df = df.drop("qt_numero_de_operacoes")
    print("10")

    output_buffer = BytesIO()
    print("11")
    df.write.parquet(output_buffer)
    del df
    print("12")

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=output_buffer.getvalue())
    print("13")
    del output_buffer

def handler():
    return lambda_handler({}, {})
