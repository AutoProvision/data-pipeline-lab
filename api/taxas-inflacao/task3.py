import os
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = f"banco-central/taxas-inflacao/{TODAY}/df.parquet"
DEST_BUCKET_NAME = os.getenv("BUCKET_CLIENT_NAME")
DEST_PATH = f'banco-central/taxas-inflacao/df.csv'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=csv_buffer.getvalue())

def handler():
    return lambda_handler({}, {})
