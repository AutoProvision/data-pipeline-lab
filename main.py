import os
import re
import traceback
from api import yaml_load
import psutil
import time
import subprocess

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

def monitor_memory_and_time(pid):
    process = psutil.Process(pid)
    max_memory = 0
    start_time = time.time()

    try:
        while process.is_running():
            memory_info = process.memory_info()
            max_memory = max(max_memory, memory_info.rss)
            time.sleep(0.1)
    except psutil.NoSuchProcess:
        pass

    end_time = time.time()
    total_time = end_time - start_time
    return total_time, max_memory

def execute_handler_in_subprocess(file_path, module_name):
    script = f"""
    import importlib.util
    spec = importlib.util.spec_from_file_location('{module_name}', '{file_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, 'handler'):
        module.handler()
    """
    process = subprocess.Popen(["python3", "-c", script], stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

    total_time, max_memory = monitor_memory_and_time(process.pid)

    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(stderr)

    return total_time, max_memory

def execute_handlers(base_path, folder_files):
    errors = {}
    usage_stats = {}

    for folder, files in folder_files.items():
        for file in files:
            print(f"### Executando handler de {folder}/{file}")
            file_path = os.path.join(base_path, folder, file)
            module_name = f"{folder}.{file[:-3]}"

            try:
                total_time, max_memory = execute_handler_in_subprocess(file_path, module_name)
                usage_stats[f"{folder}/{file}"] = {
                    "total_time": total_time,
                    "max_memory_mb": max_memory / (1024 ** 2)
                }
            except Exception as e:
                errors[f"{folder}/{file}"] = traceback.format_exc()

    return errors, usage_stats

def main():
    base_path = './api'

    yaml_load.load_yaml_as_env()

    folder_files = get_files_by_folder(base_path)
    print("Arquivos encontrados:", folder_files)

    errors, usage_stats = execute_handlers(base_path, folder_files)
    print("Erros encontrados:", errors)
    print("Estatísticas de uso:", usage_stats)

if __name__ == '__main__':
    main()
