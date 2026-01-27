import os 
import sys
from dotenv import load_dotenv
from bfs import get_folders_and_files
from blobhelper import create_folder_structure, upload_files_from_list
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential

load_dotenv()

def main() -> None:
    root = "files"
    base_path = sys.argv[1]
    folder_list, file_list = get_folders_and_files(root, base_path)
    
    #folder_list = [folder["path"].replace("\\", "/") + "/" for folder in folder_list if folder["level"] > 0]
    file_list = [file["path"].replace("\\", "/") for file in file_list if file["level"] > 0]
    file_list = [base_path + "/" + root + "/" + f for f in file_list]

    upload_files_from_list(
    account_url=os.environ["AZURE_STORAGE_ACCOUNT_URL"],
    container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
    file_paths=file_list,
    base_path=base_path + "/" + root,
    credential=ManagedIdentityCredential()
)

if __name__ == "__main__":
	main()
