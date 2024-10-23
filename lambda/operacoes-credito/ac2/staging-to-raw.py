import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import numpy as np
import boto3

s3 = boto3.client('s3')

bucket_source = 'autoprovision-ac2-staging'
bucket_dest = 'autoprovision-ac2-raw'
prefix_source = 'banco-central/operacoes-credito'

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
        'PF - Veículos'
    ]
    df = df[df['modalidade'].isin(valores_a_manter)]
    print(df.columns)
    return df

def file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def lambda_handler(event, context):
    YEAR = event['year']
    MONTHS = event['months']  # Agora recebemos uma lista de meses

    for MONTH in MONTHS:
        parquet_key = f'{prefix_source}/{YEAR}/{YEAR}-{MONTH}/planilha_{YEAR}{MONTH}.parquet'
        
        if file_exists(bucket_dest, parquet_key):
            print(f"Arquivo {parquet_key} já existe. Sem dados novos.")
            continue  # Pula para o próximo mês se o arquivo já existe

        zip_key = f'{prefix_source}/{YEAR}/planilha.zip'
        print(f'Processando arquivo: {zip_key}')
        
        zip_obj = s3.get_object(Bucket=bucket_source, Key=zip_key)
        zip_data = zip_obj['Body'].read()
        zip_file = zipfile.ZipFile(BytesIO(zip_data))

        # Aqui, consideramos que o arquivo CSV segue um padrão específico
        csv_file_name = f'planilha_{YEAR}{MONTH}.csv'
        
        if csv_file_name in zip_file.namelist():
            with zip_file.open(csv_file_name) as f:
                df = dataframefy(f)

                parquet_buffer = BytesIO()
                pq.write_table(pa.Table.from_pandas(df), parquet_buffer)
                parquet_buffer.seek(0)

                s3.upload_fileobj(parquet_buffer, bucket_dest, parquet_key)
                print(f'Arquivo {parquet_key} enviado com sucesso')
        else:
            print(f'Arquivo {csv_file_name} não encontrado no ZIP.')
                
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName='autoprovision-ac2-raw-trusted',
        InvocationType='Event'
    )

    status_code = response['StatusCode']
    if status_code == 202:
        print('Lambda chamada com sucesso.')
    else:
        print(f'Erro ao chamar a Lambda: {status_code}')

    return {
        'statusCode': 200,
        'body': 'Segunda Lambda executada e terceira Lambda chamada!',
    }
