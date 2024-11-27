import pandas as pd
import boto3
import io
from datetime import datetime
import os

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
SRC_PATH = f"banco-central/taxas-selic/{TODAY}/df.csv"
DEST_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
DEST_PATH = f'banco-central/taxas-selic/{TODAY}/df.csv'

def lambda_handler(event, context):
    response = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    csv_data = response['Body'].read()

    df = pd.read_csv(io.BytesIO(csv_data), sep=';')
    df['valor'] = df['valor'].str.replace(',', '.').astype(float)
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=csv_buffer.getvalue())

def handler():
    return lambda_handler({}, {})
