from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential
from typing import Optional
from pathlib import Path
from urllib.parse import quote

def get_blob_service_client(
    account_url: str, credential: Optional[object] = None
) -> BlobServiceClient:
    """
    Create a BlobServiceClient using DefaultAzureCredentials if no credential is provided.
    
    Args:
        account_url: The Azure Storage account URL (e.g., https://myaccount.blob.core.windows.net)
        credential: Optional credential object. If None, DefaultAzureCredential is used.
    
    Returns:
        BlobServiceClient: A client for interacting with the blob service.
    """
    if credential is None:
        credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def get_container_client(
    account_url: str, container_name: str, credential: Optional[object] = None
) -> ContainerClient:
    """
    Get a ContainerClient for a specific container.
    
    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        credential: Optional credential object. If None, DefaultAzureCredential is used.
    
    Returns:
        ContainerClient: A client for the specified container.
    """
    blob_service_client = get_blob_service_client(account_url, credential)
    return blob_service_client.get_container_client(container=container_name)


def create_folder_structure(
    account_url: str,
    container_name: str,
    folder_paths: list[str],
    credential: Optional[object] = None,
) -> None:
    """
    Create a folder structure in Azure Blob Storage by uploading empty blobs.
    
    Azure Blob Storage does not have true folders, but folder structure can be
    simulated by creating blobs with paths that include '/' separators.
    
    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        folder_paths: List of folder paths to create (e.g., ['folder1', 'folder1/subfolder']).
        credential: Optional credential object. If None, DefaultAzureCredential is used.
    """
    container_client = get_container_client(account_url, container_name, credential)
    
    for folder_path in folder_paths:
        # Ensure folder path ends with '/' to represent a folder
        blob_name = folder_path.rstrip('/') + '/.placeholder'
        
        try:
            container_client.upload_blob(name=blob_name, data=b'', overwrite=True)
            print(f"Created folder structure: {folder_path}")
        except Exception as e:
            print(f"Error creating folder {folder_path}: {e}")


def create_folder_from_path(
    account_url: str,
    container_name: str,
    path: str,
    credential: Optional[object] = None,
) -> None:
    """
    Create a single folder and all parent folders in Azure Blob Storage from a given path.
    
    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        path: The folder path to create (e.g., 'level1/level2/level3').
        credential: Optional credential object. If None, DefaultAzureCredential is used.
    """
    # Generate all parent paths
    parts = path.rstrip('/').split('/')
    paths_to_create = []
    
    for i in range(1, len(parts) + 1):
        partial_path = '/'.join(parts[:i])
        paths_to_create.append(partial_path)
    
    create_folder_structure(account_url, container_name, paths_to_create, credential)


def create_folders_from_list(
    account_url: str,
    container_name: str,
    folder_list: list[dict],
    credential: Optional[object] = None,
) -> None:
    """
    Create multiple folders from a list of folder dictionaries.
    
    Useful for syncing folder structures obtained from local traversal.
    
    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        folder_list: List of dicts with 'path' key (e.g., [{'path': 'folder1', 'level': 0}]).
        credential: Optional credential object. If None, DefaultAzureCredential is used.
    """
    folder_paths = [item['path'] for item in folder_list if 'path' in item]
    create_folder_structure(account_url, container_name, folder_paths, credential)


def upload_files_from_list(
    account_url: str,
    container_name: str,
    file_paths: list[str],
    base_path: Optional[str] = None,
    credential: Optional[object] = None,
    overwrite: bool = True,
    metadata_url_base : str = None
) -> None:
    """
    Upload multiple files from a list of local file paths to Azure Blob Storage.
    
    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        file_paths: List of local file paths to upload.
        base_path: Optional base path to remove from file paths when creating blob names.
                   If None, only the filename is used as blob name.
        credential: Optional credential object. If None, DefaultAzureCredential is used.
        overwrite: Whether to overwrite existing blobs. Default is True.
    """
    container_client = get_container_client(account_url, container_name, credential)
    
    for file_path in file_paths:
        try:
            local_file = Path(file_path)
            
            if not local_file.exists():
                print(f"Warning: File not found: {file_path}")
                continue
            
            if not local_file.is_file():
                print(f"Warning: Not a file: {file_path}")
                continue
            
            # Determine blob name
            if base_path:
                base = Path(base_path)
                try:
                    relative_path = local_file.relative_to(base)
                    blob_name = str(relative_path).replace('\\', '/')
                except ValueError:
                    # File is not relative to base_path
                    blob_name = local_file.name
            else:
                blob_name = local_file.name
            
            # Upload file
            with open(local_file, 'rb') as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=overwrite,
                    metadata={"url": metadata_url_base + "/" + quote(blob_name)} if metadata_url_base != None else None
                )
            print(f"Uploaded: {file_path} -> {blob_name}")
            
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")
