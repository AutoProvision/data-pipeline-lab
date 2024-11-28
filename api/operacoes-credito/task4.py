import boto3
import pandas as pd
from io import BytesIO
import os

s3_client = boto3.client('s3')

SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = 'banco-central/operacoes-credito/df.parquet'
DEST_BUCKET_NAME = os.getenv("BUCKET_REFINED_NAME")
DEST_PATH = 'banco-central/operacoes-credito/df.parquet'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')
    del obj

    df['vl_carteira_problematica'] = df['vl_ativo_problematico'] - df['vl_carteira_inadimplida_arrastada']

    df['vl_carteira_problematica'] = df['vl_carteira_problematica'].clip(lower=0)

    df['vl_carteira_ativa'] = df['vl_carteira_ativa'] - df['vl_carteira_problematica']
    df.rename(columns={ 'vl_carteira_ativa': 'vl_carteira_saudavel' }, inplace=True)

    df['vl_carteira_saudavel'] = df['vl_carteira_saudavel'] / df['qt_numero_de_operacoes']
    df['vl_carteira_problematica'] = df['vl_carteira_problematica'] / df['qt_numero_de_operacoes']
    df['vl_carteira_inadimplida_arrastada'] = df['vl_carteira_inadimplida_arrastada'] / df['qt_numero_de_operacoes']

    df.drop(columns=['qt_numero_de_operacoes'], inplace=True)

    output_buffer = BytesIO()
    df.to_parquet(output_buffer, index=False, engine='pyarrow')
    del df

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=output_buffer.getvalue())
    del output_buffer

def handler():
    return lambda_handler({}, {})
