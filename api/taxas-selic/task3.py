import pandas as pd
import boto3
import io
from datetime import datetime
import os

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = f"banco-central/taxas-selic/{TODAY}/df.csv"
DEST_BUCKET_NAME = os.getenv("BUCKET_CLIENT_NAME")
DEST_PATH = f'banco-central/taxas-selic/df.csv'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=';')

    df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
    df['ano_mes'] = df['data'].dt.to_period('M')
    df = df.groupby('ano_mes', as_index=False)['valor'].mean()

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=';')

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=csv_buffer.getvalue())

def handler():
    return lambda_handler({}, {})
