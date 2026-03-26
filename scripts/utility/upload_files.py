import os
import json

from blobhelper import upload_files_from_list
from azure.identity import ManagedIdentityCredential


def main():
    account_url=os.getenv("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    container_name = os.getenv("TARGET_AZURE_STORAGE_CONTAINER_NAME")
    creds = ManagedIdentityCredential()
    with open("./filelist.json", encoding="utf-8") as f:
        lst = json.load(f)

    file_paths = ["./files/".replace("\\","/") + l["path"] for l in lst]
    metadata_url_base = "/"

    upload_files_from_list(account_url, container_name, file_paths, base_path="./files/", metadata_url_base=metadata_url_base)


if __name__ == "__main__":
    main()