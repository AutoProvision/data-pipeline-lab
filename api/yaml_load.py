import yaml
import os

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
