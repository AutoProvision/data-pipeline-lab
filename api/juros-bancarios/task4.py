import pandas as pd
import boto3
import io
from datetime import datetime
import os

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = f"banco-central/juros-bancarios/{TODAY}/df.parquet"
DEST_BUCKET_NAME = os.getenv("BUCKET_CLIENT_NAME")
DEST_PATH = "banco-central/juros-bancarios/df.csv"

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=',')

    df['mes_ano'] = df['dt_referencia'].dt.strftime('%b/%Y').str.lower()
    df_grouped = df.groupby(['ct_modalidade', 'mes_ano'], as_index=False)['vl_taxa_juros_mes'].mean()
    df_pivot = df_grouped.pivot(index='ct_modalidade', columns='mes_ano', values='vl_taxa_juros_mes')
    df_pivot = df_pivot.sort_index(axis=1, key=lambda x: pd.to_datetime(x, format='%b/%Y'))

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=csv_buffer.getvalue())

def handler():
    return lambda_handler({}, {})
