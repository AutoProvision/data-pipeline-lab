import os
os.system("pip install boto3 pandas")

import pandas as pd
import boto3
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    date_str = datetime.now().strftime('%Y-%m-%d')

    source_bucket = 'autoprovision-datalake-raw'
    source_prefix = 'banco-central/taxas-inflacao/'
    destination_bucket = 'autoprovision-datalake-trusted'

    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    if 'Contents' not in response:
        return {
            'statusCode': 404,
            'body': 'Nenhum arquivo encontrado na pasta especificada.'
        }

    for obj in response['Contents']:
        source_key = obj['Key']

        if source_key.endswith('.parquet'):
            local_parquet_path = '/tmp/' + os.path.basename(source_key)
            s3_client.download_file(source_bucket, source_key, local_parquet_path)

            df = pd.read_parquet(local_parquet_path)

            df['ano'] = df['Ano'].str.extract(r'(\d+)').astype(int)
            df['meta'] = df['Meta (%)'].str.replace(',', '.').astype(float)
            df['tolerancia'] = df['Tamanhodo intervalo +/- (p.p.)'].str.replace(',', '.').astype(float)
            df['inflacao_efetiva'] = df['Inflação efetiva(Variação do IPCA, %)'].str.replace(',', '.').astype(float)

            df.drop(
                [
                    'Inflação efetiva(Variação do IPCA, %)',
                    'Tamanhodo intervalo +/- (p.p.)',
                    'Intervalode tolerância (%)',
                    'Meta (%)',
                    'Data',
                    'Norma',
                    'Ano',
                ],
                axis=1,
                inplace=True
            )

            processed_file_path = f'/tmp/arquivo-processado-{date_str}-{os.path.basename(source_key)}'
            df.to_parquet(processed_file_path, index=False)

            destination_key = f'banco-central/taxas-inflacao/{date_str}/bcb_metas_inflacao-{date_str}-trusted.parquet'
            s3_client.upload_file(processed_file_path, destination_bucket, destination_key)

    return {
        'statusCode': 200,
        'body': 'Arquivos processados e salvos com sucesso!'
    }
