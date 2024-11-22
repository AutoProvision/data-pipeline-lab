import asyncio
import json
import httpx
import boto3
import io
import zipfile
from datetime import datetime

s3_client = boto3.client('s3')

_BUCKET_NAME = 'autop-staging'
_DOMAIN = 'banco-central'
_DATASET = 'operacoes-credito'

def set_staging_path(year: str):
    staging_path = f'{_DOMAIN}/{_DATASET}/{year}/planilha.zip'
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
                s3_client.upload_fileobj(bytes_buffer, _BUCKET_NAME, file_name)

                bytes_buffer.seek(0)
                with zipfile.ZipFile(bytes_buffer, 'r') as zip_ref:
                    zip_files = zip_ref.namelist()
                    meses_presentes = {f.split('_')[1][4:6] for f in zip_files if f.startswith('planilha_') and len(f) >= 8}

                return list(meses_presentes)

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None

def lambda_handler(event, context):

    data = json.loads(event['body'])

    if 'year' in data and data['year']:
        year = data['year']
    else:
        year = datetime.now().year

    loop = asyncio.get_event_loop()
    meses_presentes = loop.run_until_complete(download_and_upload_zip(year=year))

    if meses_presentes is None:
        return {
            'statusCode': 500,
            'body': 'Erro ao processar o arquivo.'
        }

    lambda_client = boto3.client('lambda')
    payload = {
        'year': year,
        'months': meses_presentes
    }

    lambda_client.invoke(
        FunctionName='autoprovision-staging-to-raw',
        InvocationType='Event',
        Payload=json.dumps(payload).encode('utf-8')
    )

    return {
        'statusCode': 200,
        'body': 'Primeira Lambda executada e segunda Lambda chamada!',
        'months': meses_presentes
    }
