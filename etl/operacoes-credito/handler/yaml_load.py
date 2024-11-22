import yaml
import os

def load_yaml_as_env(yaml_file):
    with open(yaml_file, "r") as file:
        config = yaml.safe_load(file)

    def set_env_variables(prefix, data):
        for key, value in data.items():
            if isinstance(value, dict):
                set_env_variables(f"{prefix}_{key.upper()}", value)
            else:
                print(f"{prefix}_{key.upper()}")
                print(str(value))
                os.environ[f"{prefix}_{key.upper()}"] = str(value)

    set_env_variables("", config)

load_yaml_as_env("config.yaml")

database_url = os.getenv("BUCKET_STAGING_NAME")
api_key = os.getenv("BUCKET_RAW_NAME")
debug_mode = os.getenv("bucket_refined_name")

print(f"Database URL: {database_url}")
print(f"API Key: {api_key}")
print(f"Debug Mode: {debug_mode}")
