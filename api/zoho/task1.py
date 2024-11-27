import os
import boto3

s3_client = boto3.client('s3')

BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DB_IP = os.getenv("DB_ZOHO_INSTANCE_IP")

def lambda_handler(event, context):

    print(DB_IP)

    print("Rodando pipeline zoho...")
    print("Iniciando teste de conexão ao bucket")
    print(BUCKET_NAME)

    try:
        s3_client.list_objects(Bucket=BUCKET_NAME)
        print("Conexão ao bucket realizada com sucesso!")
    except Exception as e:
        print(f"Erro ao conectar ao bucket: {e}")
