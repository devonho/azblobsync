import re
import os 
import sys
import logging
from dotenv import load_dotenv
from bfs import get_folders_and_files
from blobhelper import create_folder_structure, upload_files_from_list
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential

load_dotenv()

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logging.getLogger("urllib3").setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)
# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1

def main() -> None:
    root = "files"
    base_path = sys.argv[1]
    folder_list, file_list = get_folders_and_files(root, base_path)
    
    #folder_list = [folder["path"].replace("\\", "/") + "/" for folder in folder_list if folder["level"] > 0]
    #file_list = [file["path"].replace("\\", "/") for file in file_list if file["level"] > 0]
    #file_list = [base_path + "/" + root + "/" + f for f in file_list]
    with open("filenames.txt",encoding="utf-8") as f:
        lines = f.readlines()
    pattern = r"\.[a-zA-Z0-9]+$"
    filenames = []
    for filename in lines:
        match = re.search(pattern, filename)
        if(match):
            filenames.append(filename)    
    file_list = [base_path + "/" + root + "/" +  f.lstrip("./").rstrip("\n") for f in filenames]

    upload_files_from_list(
        account_url=os.environ["AZURE_STORAGE_ACCOUNT_URL"],
        container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
        file_paths=file_list,
        base_path=base_path + "/" + root,
        credential=ManagedIdentityCredential(),
        metadata_url_base=os.environ["METADATA_URL_BASE"]
    )

if __name__ == "__main__":
	main()
