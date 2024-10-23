import json
import sqlalchemy
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO
import psycopg2

def lambda_handler(event, context):
    # Função para criar a conexão com o banco de dados
    create_db_connection = lambda db: sqlalchemy.create_engine(
        sqlalchemy.URL.create(
            drivername="postgresql",
            username="postgres",
            password="ak6crDhxdYYXytQm",
            host="autoprovision-postgres-db.cgpvy4hrxjmw.us-east-1.rds.amazonaws.com",
            port="5432",
            database=db
        )
    ).connect()

    # Obter o arquivo do S3
    s3 = boto3.client('s3')
    bucket_name = 'autoprovision-ac2-refined'
    key = 'banco-central/operacoes-credito/df_refined.parquet'

    # Ler o arquivo CSV do S3
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')

    # Data de referência
    df['dt_ref'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Conectando ao banco e enviando os dados
    conn = create_db_connection("data-mart-financeiro")
    df.to_sql("banco_central.operacoes_credito", con=conn, index=False, if_exists="append")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Dados inseridos com sucesso!')
    }
