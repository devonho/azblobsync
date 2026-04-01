import os
import json

from blobhelper import upload_files_from_list
from azure.identity import ManagedIdentityCredential


def main():
    account_url=os.getenv("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    container_name = os.getenv("TARGET_AZURE_STORAGE_CONTAINER_NAME")
    metadata_url_base = os.getenv("METADATA_URL_BASE")
    creds = ManagedIdentityCredential()
    base_path = os.getenv("SOURCE_LOCAL_CONTAINER_PATH")
    with open("./filelist_20260401.json", encoding="utf-8") as f:
        lst = json.load(f)

    #file_paths = ["./files/".replace("\\","/") + l["path"] for l in lst]
    file_paths = [l["path"] for l in lst]

    upload_files_from_list(account_url, container_name, file_paths, base_path=base_path, metadata_url_base=metadata_url_base, no_target_subfolders=True)


if __name__ == "__main__":
    main()