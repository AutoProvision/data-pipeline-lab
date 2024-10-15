import requests
import pandas as pd
import boto3
import io
from datetime import datetime

def lambda_handler(event, context):

    date_str = datetime.now().strftime('%Y-%m-%d')
    bucket_raw = 'autoprovision-datalake-raw'
    bucket_raw_key = f'banco-central/taxa-selic/taxa-selic-{date_str}.csv'
    bucket_trusted = 'autoprovision-datalake-trusted'
    trusted_key = f'banco-central/taxa-selic/{date_str}/taxa-selic-trusted-{date_str}.parquet'

    response = requests.get('https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv')

    if response.status_code == 200:
        csv_data = response.content

        s3 = boto3.client('s3')
        s3.upload_fileobj(io.BytesIO(csv_data), bucket_raw, bucket_raw_key)
        
        df = pd.read_csv(io.BytesIO(csv_data), sep=';')
        
        df['valor'] = df['valor'].str.replace(',', '.').astype(float)
        
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
        
        print(df)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        s3.put_object(Bucket=bucket_trusted, Key=trusted_key, Body=parquet_buffer.getvalue())
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
