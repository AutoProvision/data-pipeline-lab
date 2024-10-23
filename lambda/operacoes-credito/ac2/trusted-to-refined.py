import boto3
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'autoprovision-ac2-trusted'
    key = 'banco-central/operacoes-credito/df_trusted.parquet' 
    bucket_trusted = 'autoprovision-ac2-refined'
    output_key = 'banco-central/operacoes-credito/df_refined.parquet'

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')

    df['vl_carteira_problematica'] = df['vl_ativo_problematico'] - df['vl_carteira_inadimplida_arrastada']
    
    df[df['vl_carteira_problematica'] < 0]['vl_carteira_problematica']
    
    df['vl_carteira_problematica'] = df['vl_carteira_problematica'].clip(lower=0)
    
    df['vl_carteira_ativa'] = df['vl_carteira_ativa'] - df['vl_carteira_problematica']
    df.rename(columns={ 'vl_carteira_ativa': 'vl_carteira_saudavel' }, inplace=True)

    output_buffer = BytesIO()
    df.to_parquet(output_buffer, index=False, engine='pyarrow')

    s3.put_object(Bucket=bucket_trusted, Key=output_key, Body=output_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': 'Processamento e upload concluídos com sucesso.'
    }
