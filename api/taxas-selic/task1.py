import requests
import boto3
import io
from datetime import datetime
import os

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
DEST_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DEST_PATH = f"banco-central/taxas-selic/{TODAY}.csv"

def lambda_handler(event, context):
    response = requests.get('https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv')
    csv_data = response.content
    s3_client.upload_fileobj(io.BytesIO(csv_data), DEST_BUCKET_NAME, DEST_PATH)

def handler():
    return lambda_handler({}, {})
