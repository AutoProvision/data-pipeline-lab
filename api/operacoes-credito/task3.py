import boto3
import pandas as pd
from io import BytesIO
import os

s3_client = boto3.client('s3')

SRC_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
SRC_PATH = 'banco-central/operacoes-credito'
DEST_BUCKET = os.getenv("BUCKET_TRUSTED_NAME")
DEST_PATH = 'banco-central/operacoes-credito/df.parquet'

def lambda_handler(event, context):
    response = s3_client.list_objects_v2(Bucket=SRC_BUCKET_NAME, Prefix=SRC_PATH)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    MONETARY_COLS = ['carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico']
    CATEGORY_COLS = ['uf', 'ocupacao', 'cnae', 'porte', 'modalidade', 'indexador_modalidade', 'classificacao']
    QUANTITY_COLS = ['numero_de_operacoes']
    DATE_COLS = ['data_base']

    full_df = pd.DataFrame()

    for file in files:
        obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=file)
        df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')
        df['porte'] = df['porte'].str.strip()
        df = df[~df['porte'].isin(['PJ - Indisponível', 'PF - Indisponível'])]
        valores_a_manter = ['PJ - Capital de giro', 'PJ - Cheque especial e conta garantida', 'PF - Cartão de crédito', 'PF - Empréstimo com consignação em folha', 'PF - Habitacional', 'PF - Veículos']
        df = df[df['modalidade'].isin(valores_a_manter)]
        for column in df.columns:
            if column in MONETARY_COLS:
                df[column] = df[column].str.replace(',', '.').astype('float32')
                df = df.rename(columns={column: f'vl_{column}'})
            elif column in CATEGORY_COLS:
                df[column] = df[column].astype('category')
                df = df.rename(columns={column: f'ct_{column}'})
            elif column in QUANTITY_COLS:
                df[column] = df[column].str.replace('<= ', '').astype('int64')
                df = df.rename(columns={column: f'qt_{column}'})
            elif column in DATE_COLS:
                df[column] = pd.to_datetime(df[column], format='%Y-%m-%d')
                df = df.rename(columns={column: f'dt_{column}'})
        full_df = pd.concat([full_df, df], ignore_index=True)

    buffer = BytesIO()
    full_df.to_parquet(buffer, index=False)

    s3_client.put_object(Bucket=DEST_BUCKET, Key=DEST_PATH, Body=buffer.getvalue())

def handler():
    return lambda_handler({}, {})
