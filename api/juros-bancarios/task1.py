import boto3
import os
import requests
import pandas as pd
import io
import asyncio
import aiohttp
import json

s3_client = boto3.client('s3')

DEST_BUCKET_NAME = os.getenv("BUCKET_STAGING_NAME")
DEST_PATH = 'banco-central/juros-bancarios'

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

        task = asyncio.create_task(fetch_and_save_hist_taxas(
            session, codigoSegmento, codigoModalidade, data
        ))
        tasks.append(task)

    await asyncio.gather(*tasks)

async def main(df_datas, df_parametros):
    async with aiohttp.ClientSession() as session:
        for data in df_datas['InicioPeriodo'][::-1]:
            print(f'Recuperando arquivos de {data}...')

            await process_parametros(session, data, df_parametros)

def lambda_handler(event, context):
    df_parametros = get_parametros()
    df_datas = get_datas()

    prefix = f"{DEST_PATH}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {
        "Bucket": DEST_BUCKET_NAME,
        "Prefix": prefix,
        "Delimiter": "/",
    }

    direct_subdirectories = set()

    for page in paginator.paginate(**operation_parameters):
        if "CommonPrefixes" in page:
            for common_prefix in page["CommonPrefixes"]:
                dir_path = common_prefix["Prefix"]
                direct_subdir = dir_path[len(prefix):].rstrip("/")
                if "/" not in direct_subdir:
                    direct_subdirectories.add(direct_subdir)

    df_datas = df_datas[~df_datas['InicioPeriodo'].isin(direct_subdirectories)]

    asyncio.run(main(df_datas, df_parametros))

def handler():
    return lambda_handler({}, {})
