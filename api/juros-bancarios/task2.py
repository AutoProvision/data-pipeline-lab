import boto3
import os
import pyarrow as pa
import pandas as pd
from io import BytesIO

s3_client = boto3.client('s3')

SRC_BUCKET_NAME = os.getenv("BUCKET_STAGING_NAME")
SRC_PATH = 'banco-central/juros-bancarios'
DEST_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DEST_PATH = 'banco-central/juros-bancarios'

def list_s3_files():
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {
        'Bucket': SRC_BUCKET_NAME,
        'Prefix': SRC_PATH,
    }

    all_files = []
    for page in paginator.paginate(**operation_parameters):
        if 'Contents' in page:
            all_files.extend(obj['Key'] for obj in page['Contents'] if not obj['Key'].endswith('/'))

    return all_files

def read_s3_file(key):
    response = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=key)
    return response['Body'].read().decode('utf-8')

def lambda_handler(event, context):
    all_files = list_s3_files()
    print(all_files)
    all_files.sort()

    arrow_tables = []
    for file in all_files:
        print(f"Lendo arquivo {file}")
        content = read_s3_file(file)
        date = file.split('/')[-3]
        data = eval(content)
        df = pd.DataFrame(data['conteudo'])
        df['data'] = date
        table = pa.Table.from_pandas(df)
        arrow_tables.append(table)

    combined_table = pa.concat_tables(arrow_tables)

    df = combined_table.to_pandas()

    df.drop(['Posicao', 'TaxaJurosAoMes'], axis=1, inplace=True)

    parquet_file = BytesIO()
    df.to_parquet(parquet_file, index=False, engine='pyarrow')
    parquet_file.seek(0)

    TODAY = pd.Timestamp.today().strftime('%Y-%m-%d')
    s3_client.upload_fileobj(parquet_file, DEST_BUCKET_NAME, f"{DEST_PATH}/{TODAY}/df.parquet")
    print(f"Arquivo combinado enviado para {DEST_BUCKET_NAME}/{DEST_PATH}/{TODAY}/df.parquet")

def handler():
    return lambda_handler(None, None)
