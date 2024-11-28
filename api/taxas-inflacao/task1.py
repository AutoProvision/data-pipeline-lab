import os
import httpx
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import boto3
import io
from datetime import datetime

s3_client = boto3.client('s3')

TODAY = datetime.now().strftime('%Y-%m-%d')
DEST_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DEST_PATH = f'banco-central/taxas-inflacao/{TODAY}/df.parquet'

def lambda_handler(event, context):
    response = httpx.get('https://www.bcb.gov.br/api/paginasite/sitebcb/controleinflacao/historicometas').json()['conteudo']

    soup = BeautifulSoup(f'<div>{response}</div>', 'html.parser')
    table = soup.find('table')

    headers = []
    for th in table.find_all('th'):
        headers.append(th.text.strip())

    rows = []
    for tr in table.find_all('tr')[2:]:
        cells = []
        for td in tr.find_all('td'):
            cell_text = [
                cell.replace('\u200b', '').replace('\n', '').strip()
                for cell in td.decode_contents().split('<br/>')
            ]
            cell_text = [cell for cell in cell_text if cell]
            if len(cell_text) == 0:
                cells.append(np.nan)
            else:
                cells.append(cell_text[-1])
        rows.append(cells)

    num_cols = len(headers) if headers else max(len(row) for row in rows)

    for i, row in enumerate(rows):
        if len(row) < num_cols:
            while len(row) < num_cols:
                row.insert(1, np.nan)

    df = pd.DataFrame(rows, columns=headers if headers else None)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    buffer.seek(0)
    s3_client.upload_fileobj(buffer, DEST_BUCKET_NAME, DEST_PATH)

def handler():
    return lambda_handler({}, {})
