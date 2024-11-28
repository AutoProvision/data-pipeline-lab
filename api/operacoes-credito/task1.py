import os
import asyncio
from bs4 import BeautifulSoup
import httpx
import boto3
import io

s3_client = boto3.client('s3')

DEST_BUCKET_NAME = os.getenv("BUCKET_STAGING_NAME")

def find_latest_year():
    response = httpx.get('https://dadosabertos.bcb.gov.br/dataset/scr_data')
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    main_ul = soup.find('ul', class_='resource-list')

    li_list = main_ul.find_all('li', class_='resource-item')

    zip_url = None
    for li in li_list:
        if li is not None:
            zip_url = li.find('a', class_='resource-url-analytics').get('href')

    del response, soup, main_ul, li_list, li

    return zip_url[len(zip_url) - 8: len(zip_url) - 4]

async def download_and_upload_zip(src_path, dest_path):
    bytes_buffer = io.BytesIO()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            chunk_size = 512 * 1024
            async with client.stream('GET', src_path) as resp:
                resp.raise_for_status()

                async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                    bytes_buffer.write(chunk)
    except Exception as e:
        raise f"Erro no download do chunk: {e}"

    try:
        bytes_buffer.seek(0)
        s3_client.upload_fileobj(bytes_buffer, DEST_BUCKET_NAME, dest_path)
    except Exception as e:
        raise f"Erro no upload do chunk: {e}"

def lambda_handler(event, context):

    try:
        YEAR = find_latest_year()
    except Exception as e:
        raise f"Erro encontrando o último ano disponível: {e}"

    SRC_PATH = f"https://www.bcb.gov.br/pda/desig/planilha_{YEAR}.zip"
    DEST_PATH = f'banco-central/operacoes-credito/{YEAR}/planilha.zip'

    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_and_upload_zip(SRC_PATH, DEST_PATH))

def handler():
    # return lambda_handler({}, {})
    pass
