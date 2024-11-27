import os
import boto3

s3_client = boto3.client('s3')
ec2_client = boto3.client('ec2')

BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")

def lambda_handler(event, context):

    print("Rodando pipeline zoho...")
    print("Iniciando teste de conexão ao bucket")
    print(BUCKET_NAME)

    try:
        s3_client.list_objects(Bucket=BUCKET_NAME)
        print("Conexão ao bucket realizada com sucesso!")
    except Exception as e:
        print(f"Erro ao conectar ao bucket: {e}")

    instance_name = "ec2-autoprovision"
    filters = [
        {
            'Name': 'tag:Name',
            'Values': [instance_name]
        }
    ]

    response = ec2_client.describe_instances(Filters=filters)

    instance_details = None

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_details = {
                'InstanceId': instance['InstanceId'],
                'State': instance['State']['Name'],
                'PublicIpAddress': instance.get('PublicIpAddress', 'No Public IP')
            }

    if not instance_details:
        raise RuntimeError(f"Não foi encontrada nenhuma instância com o nome '{instance_name}'.")

    if instance_details['State'] != 'running':
        raise RuntimeError(
            f"A instância '{instance_details['InstanceId']}' com o nome '{instance_name}' não está rodando. "
            f"Estado atual: {instance_details['State']}."
        )

    print(f"Instance ID: {instance_details['InstanceId']}")
    print(f"Public IP: {instance_details['PublicIpAddress']}")

def handler():
    return lambda_handler({}, {})
