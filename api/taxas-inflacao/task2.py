import os
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
SRC_PATH = f"banco-central/taxas-inflacao/{TODAY}/df.parquet"
DEST_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
DEST_PATH = f'banco-central/taxas-inflacao/{TODAY}/df.parquet'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')

    df['ano'] = df['Ano'].str.extract(r'(\d+)').astype(int)
    df['meta'] = df['Meta (%)'].str.replace(',', '.').astype(float)
    df['tolerancia'] = df['Tamanhodo intervalo +/- (p.p.)'].str.replace(',', '.').astype(float)
    df['inflacao_efetiva'] = df['Inflação efetiva(Variação do IPCA, %)'].str.replace(',', '.').astype(float)

    df.drop(
        [
            'Inflação efetiva(Variação do IPCA, %)',
            'Tamanhodo intervalo +/- (p.p.)',
            'Intervalode tolerância (%)',
            'Meta (%)',
            'Data',
            'Norma',
            'Ano',
        ],
        axis=1,
        inplace=True
    )
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=buffer.getvalue())

def handler():
    return lambda_handler({}, {})
