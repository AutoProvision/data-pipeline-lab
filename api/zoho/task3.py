import pandas as pd
import boto3
import io
from datetime import datetime
import os

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
SRC_PATH = f"autoprovision/modelo-riscos/{TODAY}/df.csv"
DEST_BUCKET_NAME = os.getenv("BUCKET_CLIENT_NAME")
DEST_PATH = f'autoprovision/modelo-riscos/df.csv'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=',')

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=csv_buffer.getvalue())

def handler():
    return lambda_handler({}, {})
