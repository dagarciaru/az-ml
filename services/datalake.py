from azure.storage.filedatalake import DataLakeServiceClient
from os import environ

account_key =  environ.get('DATALAKE_ACCESS_KEY')
account_name = environ.get('DATALAKE_ACCOUNT_NAME')
file_system_name = environ.get('DATALAKE_CONTAINER_NAME')
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)
file_system_client = service_client.get_file_system_client(file_system_name)
file_path = environ.get("DATALAKE_BASE_FOLDER")

def get_file_client(file_name, file_folder):
    file_name_and_folder = [file_path, file_folder, file_name]
    return file_system_client.get_file_client('/'.join(filter(None, file_name_and_folder)))
    
def send_file_to_datalake(file_binary, file_name, file_folder):
    file_client = get_file_client(file_name, file_folder)
    if file_client.exists():
        file_client.delete_file()
    file_client.upload_data(file_binary, overwrite=True)

def get_file_from_datalake(file_name, file_folder):
    file_content = get_file_client(file_name, file_folder)
    try:
        return file_content.download_file().readall()
    except:
        return None

def copy_file_to_target_datalake(file_binary, file_name, file_folder,datalake_target_base_folder):   
    file_target_system = environ.get('DATALAKE_TARGET_CONTAINER_NAME')
    file_taget_system_client = service_client.get_file_system_client(file_target_system)
    file_name_and_folder = [datalake_target_base_folder, file_folder, file_name]
    file_target_client = file_taget_system_client.get_file_client('/'.join(filter(None, file_name_and_folder)))
    if file_target_client.exists():
        file_target_client.delete_file()
    file_target_client.upload_data(file_binary, overwrite=True)
