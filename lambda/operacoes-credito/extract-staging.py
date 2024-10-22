import asyncio
import json
import httpx
import boto3
import os
import io
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime

s3_client = boto3.client('s3')
BUCKET_NAME = 'autoprovision-datalake-staging'
_DOMAIN = "banco-central"
_DATASET = "operacoes-credito"

def set_staging_path(year: str):

    staging_path = f"{_DOMAIN}/{_DATASET}/{year}/planilha.zip"

    return staging_path

async def download_and_upload_zip(year: int):
    zip_url = f"https://www.bcb.gov.br/pda/desig/planilha_{year}.zip"
    file_name = set_staging_path(year)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            chunk_size = 512 * 1024
            async with client.stream('GET', zip_url) as resp:

                resp.raise_for_status() 
                bytes_buffer = io.BytesIO()

                async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                    bytes_buffer.write(chunk)

                bytes_buffer.seek(0) 
                s3_client.upload_fileobj(bytes_buffer, BUCKET_NAME, file_name)

        return {
            "statusCode": 200,
            "body": json.dumps("Arquivo ZIP baixado e salvo no S3")
        }

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Cannot download!")
        }

def lambda_handler(event, context):
    if 'body' in event:
        body = json.loads(event['body'])
        year = body.get('year')
    else:
        year = datetime.now().year

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(download_and_upload_zip(year=year))
