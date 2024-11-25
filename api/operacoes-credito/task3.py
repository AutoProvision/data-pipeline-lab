import os
os.system("pip install boto3 pandas pyarrow")

import boto3
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):

    s3 = boto3.client('s3')
    bucket_name = 'autop-raw'
    prefix = 'banco-central/operacoes-credito'
    bucket_trusted = 'autop-trusted'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    MONETARY_COLS = ['carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico']
    CATEGORY_COLS = ['uf', 'ocupacao', 'cnae', 'porte', 'modalidade', 'indexador_modalidade', 'classificacao']
    QUANTITY_COLS = ['numero_de_operacoes']
    DATE_COLS = ['data_base']

    full_df = pd.DataFrame()

    for file in files:
        obj = s3.get_object(Bucket=bucket_name, Key=file)
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
        print(file)
        full_df = pd.concat([full_df, df], ignore_index=True)



    parquet_key = f'{prefix}/df_trusted.parquet'

    buffer = BytesIO()
    full_df.to_parquet(buffer, index=False)

    s3.put_object(Bucket=bucket_trusted, Key=parquet_key, Body=buffer.getvalue())

    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName='autoprovision-trusted-to-refined',
        InvocationType='Event'
    )

    status_code = response['StatusCode']
    if status_code == 202:
        print('Lambda chamada com sucesso.')
    else:
        print(f'Erro ao chamar a Lambda: {status_code}')
