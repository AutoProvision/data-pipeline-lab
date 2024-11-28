import os
import re
import traceback
import gc
from api import yaml_load
import psutil
import time
import subprocess

def monitor_memory():
    while True:
        mem = psutil.virtual_memory()
        available_memory = mem.available / (1024 ** 2)

        print(f"Memória ainda disponível: {available_memory:.2f} MB")
        time.sleep(1)

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

def execute_handler_in_subprocess(file_path, module_name):
    script = f"""
import importlib.util
spec = importlib.util.spec_from_file_location('{module_name}', '{file_path}')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
if hasattr(module, 'handler'):
    module.handler()
"""
    result = subprocess.run(["python3", "-c", script], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)

def execute_handlers(base_path, folder_files):
    errors = {}
    for folder, files in folder_files.items():
        for file in files:
            print(f"Executando handler de {folder}/{file}")
            file_path = os.path.join(base_path, folder, file)
            module_name = f"{folder}.{file[:-3]}"

            try:
                execute_handler_in_subprocess(file_path, module_name)
            except Exception as e:
                errors[f"{folder}/{file}"] = traceback.format_exc()

            gc.collect()

    return errors

def main():
    from threading import Thread
    monitor_thread = Thread(target=monitor_memory, daemon=True)
    monitor_thread.start()

    base_path = './api'

    yaml_load.load_yaml_as_env()

    folder_files = get_files_by_folder(base_path)
    print("Arquivos encontrados:", folder_files)

    errors = execute_handlers(base_path, folder_files)
    print("Erros encontrados:", errors)

if __name__ == '__main__':
    main()
