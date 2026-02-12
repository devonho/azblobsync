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
                    metadata={"url": metadata_url_base + "/" + quote(blob_name)} if metadata_url_base != None else None,                    
                    encoding="utf-8",
                    logging_enable=True
                )
            #print(f"Uploaded: {file_path} -> {blob_name}")       
            pass     
            
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")


def compare_containers(
    source_account_url: str,
    source_container_name: str,
    target_account_url: str,
    target_container_name: str,
    source_credential: Optional[object] = None,
    target_credential: Optional[object] = None,
    prefix: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """
    Compare two blob containers (possibly in different storage accounts) and
    determine which blobs should be created, updated, or deleted in the target
    container so it matches the source container.

    Args:
        source_account_url: URL of the source storage account.
        source_container_name: Name of the source container.
        target_account_url: URL of the target storage account.
        target_container_name: Name of the target container.
        source_credential: Optional credential for the source account.
        target_credential: Optional credential for the target account.
        prefix: Optional prefix to limit comparison to blobs under this path.
        verbose: If True, print summary information.

    Returns:
        dict with keys:
            - to_create: list of blob names that exist in source but not in target
            - to_update: list of blob names that exist in both but source is newer
            - to_delete: list of blob names that exist in target but not in source
            - summary: dict with counts
    """
    src_client = get_container_client(source_account_url, source_container_name, source_credential)
    tgt_client = get_container_client(target_account_url, target_container_name, target_credential)

    # Gather blobs from source
    src_blobs: dict[str, object] = {}
    try:
        for blob in src_client.list_blobs(name_starts_with=prefix):
            src_blobs[blob.name] = blob
    except Exception as e:
        raise RuntimeError(f"Failed to list blobs in source container: {e}")

    # Gather blobs from target
    tgt_blobs: dict[str, object] = {}
    try:
        for blob in tgt_client.list_blobs(name_starts_with=prefix):
            tgt_blobs[blob.name] = blob
    except Exception as e:
        raise RuntimeError(f"Failed to list blobs in target container: {e}")

    src_names = set(src_blobs.keys())
    tgt_names = set(tgt_blobs.keys())

    to_create = sorted(list(src_names - tgt_names))
    to_delete = sorted(list(tgt_names - src_names))

    # Determine updates by comparing last_modified timestamps when blobs exist in both
    common = src_names & tgt_names
    to_update = []
    for name in common:
        s_blob = src_blobs[name]
        t_blob = tgt_blobs[name]

        # Use last_modified if available, otherwise fall back to etag/size comparison
        s_lm = getattr(s_blob, "last_modified", None)
        t_lm = getattr(t_blob, "last_modified", None)

        if s_lm is not None and t_lm is not None:
            try:
                if s_lm > t_lm:
                    to_update.append(name)
            except Exception:
                # In case comparison fails, fall back to etag
                pass
        else:
            # fallback: compare etag or size
            s_etag = getattr(s_blob, "etag", None)
            t_etag = getattr(t_blob, "etag", None)
            if s_etag and t_etag and s_etag != t_etag:
                to_update.append(name)
            else:
                s_size = getattr(s_blob, "size", None)
                t_size = getattr(t_blob, "size", None)
                if s_size is not None and t_size is not None and s_size != t_size:
                    to_update.append(name)

    to_update = sorted(to_update)

    summary = {"create": len(to_create), "update": len(to_update), "delete": len(to_delete)}

    if verbose:
        print(f"Comparison summary for prefix='{prefix}': {summary}")

    return {"to_create": to_create, "to_update": to_update, "to_delete": to_delete, "summary": summary}


def copy_blobs(
    source_account_url: str,
    source_container_name: str,
    target_account_url: str,
    target_container_name: str,
    blob_names: list[str],
    source_credential: Optional[object] = None,
    target_credential: Optional[object] = None,
    overwrite: bool = False,
    create_folders: bool = True,
    prefix: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """
    Copy blobs from a source container to a target container. Source and target
    may reside in different storage accounts. The function only processes blobs
    explicitly listed in the required `blob_names` argument.

    Args:
        source_account_url: URL of the source storage account.
        source_container_name: Name of the source container.
        target_account_url: URL of the target storage account.
        target_container_name: Name of the target container.
        blob_names: REQUIRED list of blob names to copy. Only these blobs will be
                    processed; if you want to copy all blobs, enumerate them
                    beforehand (e.g., using list_blobs).
        source_credential: Optional credential for the source account.
        target_credential: Optional credential for the target account.
        overwrite: If True, existing target blobs will be overwritten. Default False.
        create_folders: If True, create parent folder placeholders in the target
                        for blobs that include '/' in their names.
        prefix: (ignored) kept for compatibility but not used when blob_names is supplied.
        verbose: If True, print progress messages.

    Returns:
        dict with keys:
            - copied: list of blob names successfully copied
            - skipped: list of blob names skipped because target exists and overwrite is False
            - errors: dict mapping blob name to error message for failures
            - summary: dict with counts
    """
    src_container = get_container_client(source_account_url, source_container_name, source_credential)
    tgt_container = get_container_client(target_account_url, target_container_name, target_credential)

    # Only operate on blobs explicitly provided by the caller
    names = list(blob_names)

    copied: list[str] = []
    skipped: list[str] = []
    errors: dict[str, str] = {}
    created_parents: set[str] = set()

    for name in names:
        try:
            if verbose:
                print(f"Processing blob: {name}")

            tgt_blob_client = tgt_container.get_blob_client(name)

            # Check existence if not overwriting
            if not overwrite:
                try:
                    tgt_blob_client.get_blob_properties()
                    if verbose:
                        print(f"Skipping existing blob (overwrite=False): {name}")
                    skipped.append(name)
                    continue
                except Exception:
                    # Target does not exist or cannot fetch properties; proceed to copy
                    pass

            # Optionally create parent folders (simulated using placeholders)
            if create_folders and "/" in name:
                parent = "/".join(name.split("/")[:-1])
                if parent and parent not in created_parents:
                    try:
                        create_folder_from_path(target_account_url, target_container_name, parent, target_credential)
                        created_parents.add(parent)
                        if verbose:
                            print(f"Created parent placeholder for: {parent}")
                    except Exception as e:
                        # Non-fatal: continue and attempt the blob copy
                        if verbose:
                            print(f"Warning: failed to create parent placeholder '{parent}': {e}")

            # Download from source and upload to target
            src_blob_client = src_container.get_blob_client(name)
            downloader = src_blob_client.download_blob()
            data = downloader.readall()

            # Preserve metadata and content settings when possible
            try:
                props = src_blob_client.get_blob_properties()
                metadata = getattr(props, "metadata", None)
                content_settings = getattr(props, "content_settings", None)
            except Exception:
                metadata = None
                content_settings = None

            # Upload to target
            tgt_blob_client.upload_blob(
                data=data,
                overwrite=overwrite,
                metadata=metadata,
                content_settings=content_settings,
            )

            copied.append(name)
            if verbose:
                print(f"Copied blob: {name}")

        except Exception as e:
            errors[name] = str(e)
            if verbose:
                print(f"Error copying {name}: {e}")

    summary = {"copied": len(copied), "skipped": len(skipped), "errors": len(errors)}

    return {"copied": copied, "skipped": skipped, "errors": errors, "summary": summary}


def remove_placeholder_files(
    account_url: str,
    container_name: str,
    credential: Optional[object] = None,
    prefix: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Remove ".placeholder" marker blobs from a container. These are the empty
    blobs created by `create_folder_structure` to simulate folders (they end
    with '/.placeholder' or '.placeholder').

    Args:
        account_url: The Azure Storage account URL.
        container_name: The name of the container.
        credential: Optional credential object. If None, DefaultAzureCredential is used.
        prefix: Optional prefix to limit which blobs are inspected/removed.
        dry_run: If True, do not actually delete blobs, only report which would be removed.
        verbose: If True, print progress messages.

    Returns:
        dict with keys:
            - removed: list of placeholder blob names removed (or that would be removed in dry-run)
            - errors: dict mapping blob name to error message for failures
            - summary: dict with counts
    """
    container = get_container_client(account_url, container_name, credential)

    removed: list[str] = []
    errors: dict[str, str] = {}

    try:
        for blob in container.list_blobs(name_starts_with=prefix):
            # match both '/.placeholder' and top-level '.placeholder'
            if blob.name.endswith('/.placeholder') or blob.name.endswith('.placeholder'):
                if dry_run:
                    removed.append(blob.name)
                    if verbose:
                        print(f"Dry-run: would remove placeholder: {blob.name}")
                else:
                    try:
                        container.delete_blob(blob.name)
                        removed.append(blob.name)
                        if verbose:
                            print(f"Removed placeholder: {blob.name}")
                    except Exception as e:
                        errors[blob.name] = str(e)
                        if verbose:
                            print(f"Error deleting placeholder {blob.name}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed listing blobs in container: {e}")

    summary = {"removed": len(removed), "errors": len(errors)}
    return {"removed": removed, "errors": errors, "summary": summary}
