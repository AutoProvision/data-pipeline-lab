print("1")

import boto3
import os
import requests
import pandas as pd
import io
import asyncio
import aiohttp
import json
print("2")

s3_client = boto3.client('s3')

print("3")
DEST_BUCKET_NAME = os.getenv("BUCKET_STAGING_NAME")
DEST_PATH = 'banco-central/juros-bancarios'
print("4")

base_url = 'https://www.bcb.gov.br/api/servico/sitebcb'

def get_parametros():
    response = requests.get(f'{base_url}/HistoricoTaxaJurosDiario/ParametrosConsulta')
    response.raise_for_status()
    parametros = response.json()
    return pd.DataFrame(parametros['conteudo'])

def get_datas():
    response = requests.get(f'{base_url}/HistoricoTaxaJurosDiario/ConsultaDatas')
    response.raise_for_status()
    datas = response.json()
    return pd.DataFrame(datas['conteudo'])

async def get_hist_taxas(session, classificacao, modalidade, data):
	url = f"{base_url}/historicotaxajurosdiario/TodosCampos?filtro=(codigoSegmento eq '{classificacao}') and (codigoModalidade eq '{modalidade}') and (InicioPeriodo eq '{data}')"
	async with session.get(url) as response:
		return await response.json()

async def fetch_and_save_hist_taxas(session, codigoSegmento, codigoModalidade, data):
    hist_taxas = await get_hist_taxas(session, codigoSegmento, codigoModalidade, data)

    if len(hist_taxas['conteudo']) == 0:
        return

    s3_key = f'{DEST_PATH}/{data}/{codigoSegmento}-{codigoModalidade}/txjuros.json'

    with io.BytesIO() as f:
        f.write(json.dumps(hist_taxas).encode('utf-8'))
        f.seek(0)
        s3_client.upload_fileobj(f, DEST_BUCKET_NAME, s3_key)

async def process_parametros(session, data, df_parametros):
    tasks = []
    for _, parametro in df_parametros.iterrows():
        codigoSegmento = parametro['codigoSegmento']
        codigoModalidade = parametro['codigoModalidade']

        s3_key = f'{DEST_PATH}/{data}/{codigoSegmento}-{codigoModalidade}/txjuros.json'

        try:
            s3_client.head_object(Bucket=DEST_BUCKET_NAME, Key=s3_key)
            continue
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise

        task = asyncio.create_task(fetch_and_save_hist_taxas(
            session, codigoSegmento, codigoModalidade, data
        ))
        tasks.append(task)

    await asyncio.gather(*tasks)

async def main(df_datas, df_parametros):
    print("d")
    async with aiohttp.ClientSession() as session:
        print("e")
        for data in df_datas['InicioPeriodo'][::-1]:
            print("f")
            print(f'Recuperando arquivos de {data}...')

            await process_parametros(session, data, df_parametros)

def lambda_handler(event, context):
    print("a")
    df_parametros = get_parametros()
    df_datas = get_datas()
    print("b")

    main(df_datas, df_parametros)
    print("c")

def handler():
    print("a?")
    return lambda_handler({}, {})
