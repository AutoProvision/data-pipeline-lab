import boto3
import pandas as pd
from io import BytesIO
import os

s3_client = boto3.client('s3')

TODAY = pd.Timestamp.today().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
SRC_PATH = f'banco-central/juros-bancarios/{TODAY}/df.parquet'
DEST_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
DEST_PATH = f'banco-central/juros-bancarios/{TODAY}/df.parquet'

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')
    del obj

    # df = X

    output_buffer = BytesIO()
    df.to_parquet(output_buffer, index=False, engine='pyarrow')
    del df

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=output_buffer.getvalue())
    del output_buffer

def handler():
    # return lambda_handler({}, {})
    pass
