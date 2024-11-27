import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import numpy as np
import boto3
import os

s3_client = boto3.client('s3')

SRC_BUCKET_NAME = os.getenv("BUCKET_STAGING_NAME")
SRC_PATH = 'banco-central/operacoes-credito'
DEST_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DEST_PATH = 'banco-central/operacoes-credito'

def dataframefy(f):
    df = pd.read_csv(f, sep=';', encoding='utf-8-sig')
    INDEXADOR_MAP_TABLE = {
        'Prefixado': 'Pré-fixado',
        'Outros indexadores': np.nan,
        'Índices de preços': np.nan,
    }
    df['indexador_modalidade'] = df['indexador'].replace(INDEXADOR_MAP_TABLE)
    df['possui_modalidade'] = df['origem'].apply(lambda x: x != 'Sem destinação específica')
    df.drop(
        columns=[
            'cnae_subclasse',
            'a_vencer_ate_90_dias',
            'a_vencer_de_91_ate_360_dias',
            'a_vencer_de_361_ate_1080_dias',
            'a_vencer_de_1081_ate_1800_dias',
            'a_vencer_de_1801_ate_5400_dias',
            'a_vencer_acima_de_5400_dias',
            'vencido_acima_de_15_dias',
            'tcb',
            'sr',
            'indexador',
            'origem',
        ],
        inplace=True
    )

    df['porte'] = df['porte'].str.strip()
    df = df[~df['porte'].isin(['PJ - Indisponível', 'PF - Indisponível'])]
    valores_a_manter = [
        'PJ - Capital de giro',
        'PJ - Cheque especial e conta garantida',
        'PF - Cartão de crédito',
        'PF - Empréstimo com consignação em folha',
        'PF - Habitacional',
        'PF - Veículos',
    ]
    df = df[df['modalidade'].isin(valores_a_manter)]
    print(df.columns)
    return df

def file_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def lambda_handler(event, context):
    response = s3_client.list_objects_v2(Bucket=SRC_BUCKET_NAME, Prefix=SRC_PATH)

    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.zip')]
    files.sort(reverse=True)
    latest_file = files[0]

    YEAR = latest_file[-12:-8]

    zip_obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=latest_file)
    zip_data = zip_obj['Body'].read()
    zip_file = zipfile.ZipFile(BytesIO(zip_data))

    MONTHS = []
    for file in zip_file.namelist():
        if file.endswith('.csv'):
            MONTHS.append(file[-6:-4])
    
    for MONTH in MONTHS:
        parquet_key = f'{DEST_PATH}/{YEAR}/{YEAR}-{MONTH}/planilha_{YEAR}{MONTH}.parquet'

        if file_exists(DEST_BUCKET_NAME, parquet_key):
            print(f"Arquivo {parquet_key} já existe. Sem dados novos.")
            continue

        csv_file_name = f'planilha_{YEAR}{MONTH}.csv'

        if csv_file_name in zip_file.namelist():
            with zip_file.open(csv_file_name) as f:
                df = dataframefy(f)

                parquet_buffer = BytesIO()
                pq.write_table(pa.Table.from_pandas(df), parquet_buffer)
                parquet_buffer.seek(0)

                s3_client.upload_fileobj(parquet_buffer, DEST_BUCKET_NAME, parquet_key)
                print(f'Arquivo {parquet_key} enviado com sucesso')
        else:
            print(f'Arquivo {csv_file_name} não encontrado no ZIP.')

def handler():
    return lambda_handler({}, {})
