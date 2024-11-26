import yaml
import os
import boto3

s3_client = boto3.client('s3')

YAML_FILE = "config.yaml"

def load_yaml_as_env():
    with open(YAML_FILE, "r") as file:
        config = yaml.safe_load(file)

    def set_env_variables(prefix, data):
        for key, value in data.items():
            if isinstance(value, dict):
                set_env_variables(f"{prefix}{'_' if prefix != '' else ''}{key.upper()}", value)
            else:
                os.environ[f"{prefix}_{key.upper()}"] = str(value)

    set_env_variables("", config)

    print("Yaml")


    BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")

    print("Rodando pipeline zoho...")
    print("Iniciando teste de conexão ao bucket")
    print(BUCKET_NAME)

    try:
        s3_client.list_objects(Bucket=BUCKET_NAME)
        print("Conexão ao bucket realizada com sucesso!")
    except Exception as e:
        print(f"Erro ao conectar ao bucket: {e}")
