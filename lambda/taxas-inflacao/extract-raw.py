import httpx
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import boto3
import io
from datetime import datetime

def lambda_handler(event, context):   

    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client = boto3.client('s3')
    bucket_name = 'autoprovision-datalake-raw' 
    s3_key = f'banco-central/taxas-inflacao/{date_str}/bcb_metas_inflacao-{date_str}.parquet'

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

    s3_client.upload_fileobj(buffer, bucket_name, s3_key)
