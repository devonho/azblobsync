import re
import os 
import sys
import logging
from dotenv import load_dotenv
from bfs import get_folders_and_files
from blobhelper import create_folder_structure, upload_files_from_list, blob_container_source_blob_container_target
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential

load_dotenv()

if os.getenv("DEBUG", "false").lower() == "true":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

def local_source_blob_container_target() -> None:
    """
    Synchronize local files from a `files` subdirectory under a provided base path
    to an Azure Blob Storage container.

    This is meant for situations where the source files are on the local filesystem (e.g. downloaded from SharePoint)

    Behavior:
    - Reads the base path from `sys.argv[1]`.
    - Uses `get_folders_and_files(root, base_path)` to obtain the local folder and
      file listings, and additionally reads `filenames.txt` to filter which files
      to upload (files matched by extension).
    - Constructs blob names relative to the provided base path and uploads files
      with `upload_files_from_list`, setting optional metadata using
      `METADATA_URL_BASE`.
    - Authenticates using `ManagedIdentityCredential()` for the upload call.

    Environment variables:
    - AZURE_STORAGE_ACCOUNT_URL: source storage account URL
    - AZURE_STORAGE_CONTAINER_NAME: target container name
    - METADATA_URL_BASE: base URL used to populate blob metadata 'url' (optional)

    Args:
        None. The function expects a single command-line argument: the base path
        (accessible as `sys.argv[1]`).

    Raises:
        IndexError: if the base path argument is not provided on the command line.
        Propagates exceptions raised by `get_folders_and_files` or
        `upload_files_from_list`.

    Returns:
        None
    """
    
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

def blob_container_source_blob_container_target_main() -> None:
    """
    Wrapper to synchronize blobs between two blob containers (possibly in
    different storage accounts) using environment variables and managed identity.

    Environment variables used (with fallbacks):
    - SOURCE_AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_ACCOUNT_URL
    - SOURCE_AZURE_STORAGE_CONTAINER_NAME or AZURE_STORAGE_CONTAINER_NAME
    - TARGET_AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_ACCOUNT_URL
    - TARGET_AZURE_STORAGE_CONTAINER_NAME or AZURE_STORAGE_CONTAINER_NAME
    - SYNC_PREFIX (optional): prefix to limit sync
    - OVERWRITE_UPDATES (optional): 'true'/'false' (default 'true')
    - DELETE_EXTRANEOUS (optional): 'true'/'false' (default 'false')

    The function prints a summary of actions or errors.
    """
    src_url = os.environ.get("SOURCE_AZURE_STORAGE_ACCOUNT_URL") or os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
    src_container = os.environ.get("SOURCE_AZURE_STORAGE_CONTAINER_NAME") or os.environ.get("AZURE_STORAGE_CONTAINER_NAME")
    tgt_url = os.environ.get("TARGET_AZURE_STORAGE_ACCOUNT_URL") or os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
    tgt_container = os.environ.get("TARGET_AZURE_STORAGE_CONTAINER_NAME") or os.environ.get("AZURE_STORAGE_CONTAINER_NAME")

    if not src_url or not src_container or not tgt_url or not tgt_container:
        raise KeyError("Missing required environment variables for source/target account or container names")

    prefix = os.environ.get("SYNC_PREFIX")
    overwrite_updates = os.environ.get("OVERWRITE_UPDATES", "true").lower() == "true"
    delete_extraneous = os.environ.get("DELETE_EXTRANEOUS", "false").lower() == "true"

    result = blob_container_source_blob_container_target(
        source_account_url=src_url,
        source_container_name=src_container,
        target_account_url=tgt_url,
        target_container_name=tgt_container,
        source_credential=ManagedIdentityCredential(),
        target_credential=ManagedIdentityCredential(),
        prefix=prefix,
        overwrite_updates=overwrite_updates,
        create_folders=True,
        delete_extraneous=delete_extraneous,
        verbose=True,
    )

    print("Sync result summary:", result.get("summary", {}))


def main() -> None:
    pass

if __name__ == "__main__":
	main()
