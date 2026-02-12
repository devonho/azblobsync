import re
import os 
import sys
import logging
from dotenv import load_dotenv
from bfs import get_folders_and_files
from blobhelper import create_folder_structure, upload_files_from_list, compare_containers, copy_blobs, get_container_client, remove_placeholder_files
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from azure.storage.blob import StorageSharedKeyCredential
from urllib.parse import urlparse
import time
from datetime import datetime, timedelta

# Prefer AzureNamedKeyCredential if available (newer API); otherwise use StorageSharedKeyCredential
try:
    from azure.core.credentials import AzureNamedKeyCredential as _AzureNamedKeyCredential
    _NAMED_KEY_CLS = _AzureNamedKeyCredential
except Exception:
    try:
        from azure.storage.blob import StorageSharedKeyCredential as _StorageKeyCred
        _NAMED_KEY_CLS = _StorageKeyCred
    except Exception:
        _NAMED_KEY_CLS = None

load_dotenv()


logger = logging.getLogger("azblobsync")

# Configure a root logging handler with timestamp and file information
log_level = logging.DEBUG if os.getenv("DEBUG", "false").lower() == "true" else logging.INFO
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(log_level)

if os.getenv("DEBUG", "false").lower() == "true":
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # logging.getLogger("urllib3").setLevel(logging.DEBUG)
    # requests_log = logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)
    # import http.client as http_client
    # http_client.HTTPConnection.debuglevel = 1
    pass


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

def blob_container_source_blob_container_target_main(delete_extraneous: bool | None = None) -> None:
    """
    Wrapper to synchronize blobs between two blob containers (possibly in
    different storage accounts) using environment variables and managed identity.

    Args:
        delete_extraneous: Optional override to control whether blobs present
            in the target but not in the source should be deleted. If None,
            the value of the environment variable DELETE_EXTRANEOUS is used.

    Environment variables used (with fallbacks):
    - SOURCE_AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_ACCOUNT_URL
    - SOURCE_AZURE_STORAGE_CONTAINER_NAME or AZURE_STORAGE_CONTAINER_NAME
    - TARGET_AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_ACCOUNT_URL
    - TARGET_AZURE_STORAGE_CONTAINER_NAME or AZURE_STORAGE_CONTAINER_NAME
    - SYNC_PREFIX (optional): prefix to limit sync
    - OVERWRITE_UPDATES (optional): 'true'/'false' (default 'true')
    - DELETE_EXTRANEOUS (optional): 'true'/'false' (default 'false') unless
      overridden by the `delete_extraneous` argument.

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
    verbose = os.environ.get("DEBUG", "false").lower() == "true"

    src_cred = None
    tgt_cred = None

    def _build_credential_from_env(key_env: str, account_url: str):
        """Return StorageSharedKeyCredential if env key present, else ManagedIdentityCredential()."""
        key = os.environ.get(key_env)
        if key:
            try:
                parsed = urlparse(account_url)
                host = parsed.netloc
                account_name = host.split(".")[0]
            except Exception:
                account_name = None
            if not account_name:
                logger.warning("Could not parse account name from URL '%s', falling back to ManagedIdentityCredential", account_url)
                return ManagedIdentityCredential()
            logger.info("Using StorageSharedKeyCredential for account '%s' from env %s", account_name, key_env)
            return StorageSharedKeyCredential(account_name, key)
        return ManagedIdentityCredential()

    src_cred = _build_credential_from_env("SOURCE_AZURE_STORAGE_CONTAINER_KEY", src_url)
    tgt_cred = _build_credential_from_env("TARGET_AZURE_STORAGE_CONTAINER_KEY", tgt_url)

    # Compare containers
    comp = compare_containers(
        src_url,
        src_container,
        tgt_url,
        tgt_container,
        source_credential=src_cred,
        target_credential=tgt_cred,
        prefix=prefix,
        verbose=verbose,
    )

    to_create = comp.get("to_create", [])
    to_update = comp.get("to_update", [])
    to_delete = comp.get("to_delete", [])

    logger.info("Compare result: create=%d update=%d delete=%d", len(to_create), len(to_update), len(to_delete))

    copy_create_result = None
    copy_update_result = None

    if to_create:
        copy_create_result = copy_blobs(
            src_url,
            src_container,
            tgt_url,
            tgt_container,
            blob_names=to_create,
            source_credential=src_cred,
            target_credential=tgt_cred,
            overwrite=False,
            create_folders=True,
            verbose=verbose,
        )

    if to_update:
        copy_update_result = copy_blobs(
            src_url,
            src_container,
            tgt_url,
            tgt_container,
            blob_names=to_update,
            source_credential=src_cred,
            target_credential=tgt_cred,
            overwrite=bool(overwrite_updates),
            create_folders=True,
            verbose=verbose,
        )

    deleted = []
    delete_errors = {}
    # Allow function parameter to override environment variable
    if delete_extraneous is None:
        delete_extraneous = os.environ.get("DELETE_EXTRANEOUS", "false").lower() == "true"
    if delete_extraneous and to_delete:
        tgt_client = get_container_client(tgt_url, tgt_container, tgt_cred)
        for name in to_delete:
            try:
                tgt_client.delete_blob(name)
                deleted.append(name)
            except Exception as e:
                delete_errors[name] = str(e)

    # Remove placeholder files created to represent folders in the target container
    try:
        remove_result = remove_placeholder_files(
            account_url=tgt_url,
            container_name=tgt_container,
            credential=tgt_cred,
            prefix=prefix,
            dry_run=False,
            verbose=verbose,
        )
        logger.info("Removed placeholders: %s", remove_result.get('summary', {}))
    except Exception as e:
        # Non-fatal: report and continue
        logger.warning("Warning: failed to remove placeholder files: %s", e)

    summary = {
        "compare": comp.get("summary", {}),
        "created_copied": copy_create_result.get("summary", {}).get("copied", 0) if copy_create_result else 0,
        "updated_copied": copy_update_result.get("summary", {}).get("copied", 0) if copy_update_result else 0,
        "deleted": len(deleted),
        "delete_errors": len(delete_errors),
    }

    logger.info("Sync result summary: %s", summary)
    return {"comparison": comp, "copy_create": copy_create_result, "copy_update": copy_update_result, "deleted": deleted, "delete_errors": delete_errors, "summary": summary}

def main() -> None:
    """
    Run sync once or in a scheduling loop controlled by LOOP_INTERVAL_MINUTES.

    If environment variable `LOOP_INTERVAL_MINUTES` is set to a positive number,
    the function will run `blob_container_source_blob_container_target_main()` repeatedly
    every N minutes until interrupted. If not set or set to 0, a single run is executed.

    If LOOP_START_DAY_OF_WEEK or LOOP_START_TIME_OF_DAY are set, the first scheduled
    run will be delayed until the next occurrence matching the configured day and time.
    """
    interval_env = os.environ.get("LOOP_INTERVAL_MINUTES")
    interval_minutes = 0.0
    if interval_env:
        try:
            interval_minutes = float(interval_env)
        except Exception:
            logger.warning("Invalid LOOP_INTERVAL_MINUTES value '%s', defaulting to single run", interval_env)
            interval_minutes = 0.0

    # Optional scheduled start constraints
    start_day_env = os.environ.get("LOOP_START_DAY_OF_WEEK")  # e.g. 'mon', 'monday', '0' (Mon=0)
    start_time_env = os.environ.get("LOOP_START_TIME_OF_DAY")  # e.g. '14:30' (24h HH:MM)

    def _parse_weekday(val: str):
        if not val:
            return None
        val = val.strip().lower()
        weekdays = {
            'monday': 0, 'mon': 0,
            'tuesday': 1, 'tue': 1, 'tues': 1,
            'wednesday': 2, 'wed': 2,
            'thursday': 3, 'thu': 3, 'thurs': 3,
            'friday': 4, 'fri': 4,
            'saturday': 5, 'sat': 5,
            'sunday': 6, 'sun': 6,
        }
        if val in weekdays:
            return weekdays[val]
        try:
            n = int(val)
            if 0 <= n <= 6:
                return n
        except Exception:
            pass
        logger.warning("Unrecognized LOOP_START_DAY_OF_WEEK value: '%s'", val)
        return None

    def _parse_time_of_day(val: str):
        if not val:
            return None
        try:
            parts = val.strip().split(":")
            if len(parts) != 2:
                raise ValueError()
            hh = int(parts[0])
            mm = int(parts[1])
            if not (0 <= hh < 24 and 0 <= mm < 60):
                raise ValueError()
            return hh, mm
        except Exception:
            logger.warning("Unrecognized LOOP_START_TIME_OF_DAY value: '%s' (expected HH:MM 24h)", val)
            return None

    start_weekday = _parse_weekday(start_day_env)
    start_time_hm = _parse_time_of_day(start_time_env)

    if interval_minutes > 0:
        # If a constrained start is configured, compute wait until that start
        if start_weekday is not None or start_time_hm is not None:
            now = datetime.now()
            target_hhmm = start_time_hm if start_time_hm is not None else (now.hour, now.minute)
            # compute candidate day
            if start_weekday is None:
                # only time specified: next occurrence of that time (today if future else tomorrow)
                candidate = now.replace(hour=target_hhmm[0], minute=target_hhmm[1], second=0, microsecond=0)
                if candidate <= now:
                    candidate = candidate + timedelta(days=1)
            else:
                # weekday specified (possibly with time)
                days_ahead = (start_weekday - now.weekday()) % 7
                candidate = (now + timedelta(days=days_ahead)).replace(hour=target_hhmm[0], minute=target_hhmm[1], second=0, microsecond=0)
                if candidate <= now:
                    candidate = candidate + timedelta(days=7)

            wait_seconds = (candidate - now).total_seconds()
            if wait_seconds > 0:
                logger.info("Delaying scheduler start until %s (in %.1f seconds)", candidate.isoformat(sep=' '), wait_seconds)
                try:
                    time.sleep(wait_seconds)
                except KeyboardInterrupt:
                    logger.info("Startup wait interrupted by user, exiting")
                    return

        interval_seconds = interval_minutes * 60.0
        logger.info("Starting scheduler: running sync every %s minutes", interval_minutes)
        try:
            while True:
                logger.info("Scheduled run: starting sync")
                try:
                    blob_container_source_blob_container_target_main()
                except Exception:
                    logger.exception("Scheduled sync run failed")
                logger.info("Scheduled run: sleeping for %s minutes", interval_minutes)
                try:
                    time.sleep(interval_seconds)
                except KeyboardInterrupt:
                    logger.info("Scheduler interrupted by user, exiting")
                    break
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user, exiting")
    else:
        logger.info("Running single sync invocation")
        try:
            blob_container_source_blob_container_target_main()
        except Exception:
            logger.exception("Sync failed")
            raise

if __name__ == "__main__":
    main()
