"""
Local filesystem helper utilities for azblobsync.

Provides a function to compare a local folder against an Azure Blob container
and return lists of files to create, update or delete in the target container
so the container mirrors the local folder.

The function reads the following environment variables by default (can be
overridden by passing parameters):
- LOCAL_CONTAINER_PATH
- TARGET_AZURE_STORAGE_ACCOUNT_URL
- TARGET_AZURE_STORAGE_CONTAINER_NAME

"""
from pathlib import Path
import os
from datetime import datetime, timezone
from typing import Optional
import logging

from blobhelper import get_container_client

logger = logging.getLogger("azblobsync")


def compare_local_to_container(
    local_path: Optional[str] = None,
    target_account_url: Optional[str] = None,
    target_container_name: Optional[str] = None,
    credential: Optional[object] = None,
    prefix: Optional[str] = None,
) -> dict:
    """
    Compare files in a local folder to blobs in a target Azure Blob container.

    Returns a dict with keys 'to_create', 'to_update', 'to_delete', and
    'summary' similar to `compare_containers` in `blobhelper`.

    Behavior:
    - local_path: path to the local folder containing files to sync. If not
      provided, the function reads the `LOCAL_CONTAINER_PATH` environment
      variable.
    - target_account_url and target_container_name: if not provided the
      function reads `TARGET_AZURE_STORAGE_ACCOUNT_URL` and
      `TARGET_AZURE_STORAGE_CONTAINER_NAME` environment variables.
    - prefix: if provided, limits both local and remote entries to blob names
      beginning with this prefix.

    Comparison logic:
    - For files present in both local and target: compare local file's
      modification time (mtime) to the blob's last_modified. If local mtime is
      strictly greater, the file is considered an update. If last_modified is
      not available, the function falls back to comparing sizes.

    Returns:
        {
            "to_create": [...],
            "to_update": [...],
            "to_delete": [...],
            "summary": {"create": N, "update": M, "delete": K}
        }
    """
    # Resolve environment defaults
    if not local_path:
        local_path = os.environ.get("LOCAL_CONTAINER_PATH")
    if not target_account_url:
        target_account_url = os.environ.get("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    if not target_container_name:
        target_container_name = os.environ.get("TARGET_AZURE_STORAGE_CONTAINER_NAME")

    if not local_path:
        raise KeyError("LOCAL_CONTAINER_PATH must be provided either as an argument or environment variable")
    if not target_account_url or not target_container_name:
        raise KeyError("TARGET_AZURE_STORAGE_ACCOUNT_URL and TARGET_AZURE_STORAGE_CONTAINER_NAME must be provided")

    base = Path(local_path)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"Local path does not exist or is not a directory: {local_path}")

    # Gather local files
    local_files: dict[str, dict] = {}
    for root, _, files in os.walk(base):
        for fname in files:
            full = Path(root) / fname
            try:
                rel = full.relative_to(base)
            except Exception:
                # Should not happen, but skip if it does
                continue
            blob_name = str(rel).replace("\\", "/")
            if prefix and not blob_name.startswith(prefix):
                continue
            stat = full.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            size = stat.st_size
            local_files[blob_name] = {"mtime": mtime, "size": size}

    # Gather blobs from target container
    tgt_client = get_container_client(target_account_url, target_container_name, credential)
    tgt_blobs: dict[str, object] = {}
    try:
        for blob in tgt_client.list_blobs(name_starts_with=prefix):
            # Skip tombstoned / deleted entries if present in the listing metadata
            if getattr(blob, "deleted", False):
                logger.debug("Skipping deleted blob in target listing: %s", getattr(blob, "name", "<unknown>"))
                continue
            # Skip placeholder blobs used to represent folders
            if getattr(blob, "name", "").endswith('/.placeholder') or getattr(blob, "name", "").endswith('.placeholder'):
                logger.debug("Skipping placeholder blob in target listing: %s", blob.name)
                continue
            tgt_blobs[blob.name] = blob
    except Exception as e:
        raise RuntimeError(f"Failed to list blobs in target container: {e}")

    local_names = set(local_files.keys())
    tgt_names = set(tgt_blobs.keys())

    to_create = sorted(list(local_names - tgt_names))
    to_delete = sorted(list(tgt_names - local_names))

    common = local_names & tgt_names
    to_update = []
    for name in common:
        local_meta = local_files[name]
        t_blob = tgt_blobs[name]

        t_lm = getattr(t_blob, "last_modified", None)
        if t_lm is not None:
            # Ensure both are timezone-aware for comparison
            try:
                if local_meta["mtime"] > t_lm:
                    to_update.append(name)
            except Exception:
                # Fallback to size
                t_size = getattr(t_blob, "size", None)
                if t_size is not None and local_meta["size"] != t_size:
                    to_update.append(name)
        else:
            t_size = getattr(t_blob, "size", None)
            if t_size is not None and local_meta["size"] != t_size:
                to_update.append(name)

    to_update = sorted(to_update)

    summary = {"create": len(to_create), "update": len(to_update), "delete": len(to_delete)}
    logger.info("Local->container comparison summary for local='%s' prefix='%s': %s", local_path, prefix, summary)

    return {"to_create": to_create, "to_update": to_update, "to_delete": to_delete, "summary": summary}
