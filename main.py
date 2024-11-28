import os
import re
import importlib.util
import traceback
import gc
from api import yaml_load
import psutil

def print_memory_usage():
    process = psutil.Process(os.getpid())
    print(f"Uso de memória: {process.memory_info().rss / 1024 ** 2:.2f} MB")

def get_files_by_folder(base_path):
    folder_files = {}
    task_pattern = re.compile(r"task\d+\.py")

    for root, dirs, files in os.walk(base_path):
        if root == base_path:
            for folder in dirs:
                folder_path = os.path.join(base_path, folder)
                curr_folder_files = [
                    f for f in os.listdir(folder_path) if task_pattern.match(f)
                ]
                curr_folder_files.sort()
                folder_files[folder] = curr_folder_files
    return folder_files

def execute_handlers(base_path, folder_files):
    errors = {}
    for folder, files in folder_files.items():
        for file in files:
            print(f"Executando handler de {folder}/{file}")
            file_path = os.path.join(base_path, folder, file)
            module_name = f"{folder}.{file[:-3]}"

            try:
                print_memory_usage()
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                print_memory_usage()

                if hasattr(module, 'handler'):
                    module.handler()
                else:
                    errors[f"{folder}/{file}"] = "handler() not found"
                print_memory_usage()
                del module
                if module_name in sys.modules:
                    del sys.modules[module_name]
                print_memory_usage()

            except Exception as e:
                errors[f"{folder}/{file}"] = traceback.format_exc()
            
            gc.collect()

    return errors

def main():
    base_path = './api'

    yaml_load.load_yaml_as_env()

    folder_files = get_files_by_folder(base_path)
    print("Arquivos encontrados:", folder_files)

    errors = execute_handlers(base_path, folder_files)
    print("Erros encontrados:", errors)

if __name__ == '__main__':
    main()
