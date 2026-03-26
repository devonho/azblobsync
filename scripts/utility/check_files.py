import os
import json

from blobhelper import get_container_client
from azure.identity import ManagedIdentityCredential


def list_container(account_url, container_name, creds):
    client = get_container_client(account_url, container_name, creds)
    keys = ['name', 'container', 'snapshot', 'version_id', 'is_current_version', 'blob_type', 'metadata', 'last_modified', 'size', 'deleted', 'deleted_time', 'creation_time', ]

    blob_list = []
    for blob in client.list_blobs():
        blob_list.append({k:(blob[k].isoformat() if (blob[k] != None and (k[-5:] == "_time" or k[-9:] == "_modified")) else blob[k]) for k in keys} )
    return blob_list


def main():
    account_url=os.getenv("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    container_name = os.getenv("TARGET_AZURE_STORAGE_CONTAINER_NAME")
    creds = ManagedIdentityCredential()

    blob_list = list_container(account_url, container_name, creds)

    with open("./output.json", "w", encoding="utf-8") as f:
        json.dump(blob_list, f)


if __name__ == "__main__":
    main()