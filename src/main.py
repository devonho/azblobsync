import re
import os 
import sys
import logging
from dotenv import load_dotenv
from blobhelper import create_folder_structure, upload_files_from_list, compare_containers, copy_blobs, get_container_client, remove_placeholder_files
from localfshelper import compare_local_to_container
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from azure.core.credentials import AzureNamedKeyCredential
from urllib.parse import urlparse
from pathlib import Path
import time
from datetime import datetime, timedelta

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


def local_source_blob_container_target() -> dict:
    """
    Synchronize local files from a folder to an Azure Blob Storage container using
    a comparison step to determine which files to create, update, or delete.

    This updated implementation uses `compare_local_to_container()` from
    `src/localfshelper.py` to compute diffs and then uploads files that need
    to be created or updated using `upload_files_from_list`.

    Environment variables consulted (can be overridden by command-line arg):
    - LOCAL_CONTAINER_PATH: local folder containing files to sync (preferred)
    - TARGET_AZURE_STORAGE_ACCOUNT_URL
    - TARGET_AZURE_STORAGE_CONTAINER_NAME
    - SYNC_PREFIX (optional): limit comparison/upload to paths starting with this prefix
    - METADATA_URL_BASE (optional): passed through to uploader

    Returns:
        dict: result containing the comparison and lists of uploaded files.
    """
    # prefer explicit env var, otherwise allow a command-line arg for convenience
    local_path = os.environ.get("LOCAL_CONTAINER_PATH")
    if not local_path:
        if len(sys.argv) > 1:
            local_path = sys.argv[1]
        else:
            raise KeyError("LOCAL_CONTAINER_PATH must be provided as env var or first CLI argument")

    target_account_url = os.environ.get("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    target_container = os.environ.get("TARGET_AZURE_STORAGE_CONTAINER_NAME")
    if not target_account_url or not target_container:
        raise KeyError("TARGET_AZURE_STORAGE_ACCOUNT_URL and TARGET_AZURE_STORAGE_CONTAINER_NAME must be set")

    prefix = os.environ.get("SYNC_PREFIX")

    # Build credential: prefer key-based AzureNamedKeyCredential if env var present
    key = os.environ.get("TARGET_AZURE_STORAGE_CONTAINER_KEY")
    if key:
        try:
            parsed = urlparse(target_account_url)
            host = parsed.netloc
            account_name = host.split(".")[0] if host else None
        except Exception:
            account_name = None
        if account_name:
            logger.info("Using AzureNamedKeyCredential for target account '%s' from env TARGET_AZURE_STORAGE_CONTAINER_KEY", account_name)
            target_cred = AzureNamedKeyCredential(account_name, key)
        else:
            logger.warning("Could not parse account name from URL '%s', falling back to ManagedIdentityCredential", target_account_url)
            target_cred = ManagedIdentityCredential()
    else:
        target_cred = ManagedIdentityCredential()

    # perform comparison using the selected credential
    try:
        comp = compare_local_to_container(
            local_path=local_path,
            target_account_url=target_account_url,
            target_container_name=target_container,
            credential=target_cred,
            prefix=prefix,
        )
    except Exception as e:
        logger.exception("Local->container comparison failed: %s", e)
        raise

    to_create = comp.get("to_create", [])
    to_update = comp.get("to_update", [])
    to_delete = comp.get("to_delete", [])

    logger.info("Local->container compare: create=%d update=%d delete=%d", len(to_create), len(to_update), len(to_delete))

    uploaded = []
    upload_errors = {}
    skipped_updates = []
    skipped_by_skip_copy = []

    # If SKIP_COPY is set to 'true', skip uploading all create candidates
    if os.environ.get("SKIP_COPY", "true").lower() == "true":
        if to_create:
            skipped_by_skip_copy = list(to_create)
            logger.info("SKIP_COPY=true: skipping %d create candidates", len(skipped_by_skip_copy))
            to_create = []

    # Respect SKIP_UPDATES env var: when false, do not upload update candidates
    SKIP_UPDATES = os.environ.get("SKIP_UPDATES", "true").lower() == "true"
    if not SKIP_UPDATES and to_update:
        skipped_updates = list(to_update)
        logger.info("SKIP_UPDATES=false: skipping %d update candidates", len(skipped_updates))

    # Upload files that need to be created or updated (depending on overwrite flag)
    names_to_upload = to_create + (to_update if SKIP_UPDATES else [])
    if names_to_upload:
        file_paths = []
        for name in names_to_upload:
            file_paths.append(str(Path(local_path) / Path(name)))

        try:
            upload_files_from_list(
                account_url=target_account_url,
                container_name=target_container,
                file_paths=file_paths,
                base_path=local_path,
                credential=target_cred,
                overwrite=True,
                metadata_url_base=os.environ.get("METADATA_URL_BASE"),
            )
            uploaded = list(names_to_upload)
            logger.info("Uploaded %d files (create+update)", len(uploaded))
        except Exception as e:
            logger.exception("Error uploading local files: %s", e)
            for name in names_to_upload:
                upload_errors[name] = str(e)

    # Handle deletions: if configured, remove blobs present in the target but missing locally
    deleted = []
    delete_errors = {}
    SKIP_DELETE = os.environ.get("SKIP_DELETE", "true").lower() == "true"
    if (SKIP_DELETE == False) and to_delete:
        try:
            tgt_client = get_container_client(target_account_url, target_container, target_cred)
            for name in to_delete:
                try:
                    tgt_client.delete_blob(name)
                    deleted.append(name)
                    logger.info("Deleted target blob not present locally: %s", name)
                except Exception as e:
                    delete_errors[name] = str(e)
                    logger.error("Failed to delete target blob %s: %s", name, e)
        except Exception as e:
            # If listing or client creation failed, surface as error
            logger.exception("Failed to delete extraneous blobs: %s", e)
    else:
        if to_delete:
            logger.info("SKIP_DELETE is true: skipping deletion of %d extraneous target blobs", len(to_delete))

    result = {
        "comparison": comp,
        "uploaded": uploaded,
        "upload_errors": upload_errors,
        "to_delete": to_delete,
        "skipped_updates": skipped_updates,
        "skipped_by_skip_copy": skipped_by_skip_copy,
        "deleted": deleted,
        "delete_errors": delete_errors,
    }

    return result

def blob_container_source_blob_container_target_main(SKIP_DELETE: bool | None = None) -> None:
    """
    Wrapper to synchronize blobs between two blob containers (possibly in
    different storage accounts) using environment variables and managed identity.

    Args:
        SKIP_DELETE: Optional override to control whether blobs present
            in the target but not in the source should be deleted. If None,
            the value of the environment variable SKIP_DELETE is used.

    Environment variables used:
    - SOURCE_AZURE_STORAGE_ACCOUNT_URL
    - SOURCE_AZURE_STORAGE_CONTAINER_NAME
    - TARGET_AZURE_STORAGE_ACCOUNT_URL
    - TARGET_AZURE_STORAGE_CONTAINER_NAME
    - SYNC_PREFIX (optional): prefix to limit sync
    - SKIP_UPDATES (optional): 'true'/'false' (default 'true')
    - SKIP_DELETE (optional): 'true'/'false' (default 'false') unless
      overridden by the `SKIP_DELETE` argument.

    The function prints a summary of actions or errors.
    """
    src_url = os.environ.get("SOURCE_AZURE_STORAGE_ACCOUNT_URL")
    src_container = os.environ.get("SOURCE_AZURE_STORAGE_CONTAINER_NAME")
    tgt_url = os.environ.get("TARGET_AZURE_STORAGE_ACCOUNT_URL")
    tgt_container = os.environ.get("TARGET_AZURE_STORAGE_CONTAINER_NAME")

    if not src_url or not src_container or not tgt_url or not tgt_container:
        raise KeyError("Missing required environment variables for source/target account or container names")

    prefix = os.environ.get("SYNC_PREFIX")
    SKIP_UPDATES = os.environ.get("SKIP_UPDATES", "true").lower() == "true"
    SKIP_DELETE = os.environ.get("SKIP_DELETE", "true").lower() == "true"
    verbose = os.environ.get("DEBUG", "false").lower() == "true"

    src_cred = None
    tgt_cred = None

    def _build_credential_from_env(key_env: str, account_url: str):
        """Return AzureNamedKeyCredential if env key present, else ManagedIdentityCredential()."""
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
            logger.info("Using AzureNamedKeyCredential for account '%s' from env %s", account_name, key_env)
            return AzureNamedKeyCredential(account_name, key)
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

    # Support SKIP_COPY env var: when true skip creating new blobs (to_create)
    skipped_by_skip_copy = []
    if os.environ.get("SKIP_COPY", "true").lower() == "true":
        if to_create:
            skipped_by_skip_copy = list(to_create)
            logger.info("SKIP_COPY=true: skipping %d create candidates", len(skipped_by_skip_copy))
            to_create = []

    copy_create_result = None
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

    copy_update_result = None
    if not SKIP_UPDATES and to_update:
        copy_update_result = copy_blobs(
            src_url,
            src_container,
            tgt_url,
            tgt_container,
            blob_names=to_update,
            source_credential=src_cred,
            target_credential=tgt_cred,
            overwrite=bool(SKIP_UPDATES),
            create_folders=True,
            verbose=verbose,
        )

    deleted = []
    delete_errors = {}
    # Allow function parameter to override environment variable
    if SKIP_DELETE is None:
        SKIP_DELETE = os.environ.get("SKIP_DELETE", "true").lower() == "true"
    if (SKIP_DELETE == False) and to_delete:
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
    return {"comparison": comp, "copy_create": copy_create_result, "copy_update": copy_update_result, "deleted": deleted, "delete_errors": delete_errors, "skipped_by_skip_copy": skipped_by_skip_copy, "summary": summary}

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

    # Decide which sync mode to run based on environment variables
    has_source_container = os.environ.get("SOURCE_AZURE_STORAGE_CONTAINER_NAME") is not None
    has_local_path = os.environ.get("LOCAL_CONTAINER_PATH") is not None

    if has_source_container and has_local_path:
        logger.error("Configuration error: both SOURCE_AZURE_STORAGE_CONTAINER_NAME and LOCAL_CONTAINER_PATH are set; please set only one mode")
        raise KeyError("Both SOURCE_AZURE_STORAGE_CONTAINER_NAME and LOCAL_CONTAINER_PATH are set; choose only one mode")

    if has_source_container:
        selected_sync_func = blob_container_source_blob_container_target_main
        logger.info("Selected sync mode: container-to-container (SOURCE_AZURE_STORAGE_CONTAINER_NAME present)")
    elif has_local_path:
        selected_sync_func = local_source_blob_container_target
        logger.info("Selected sync mode: local->container (LOCAL_CONTAINER_PATH present)")
    else:
        # Default to container-to-container sync if no explicit mode is set
        selected_sync_func = blob_container_source_blob_container_target_main
        logger.info("No mode env var explicitly set; defaulting to container-to-container sync")

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
                    selected_sync_func()
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
            selected_sync_func()
        except Exception:
            logger.exception("Sync failed")
            raise

if __name__ == "__main__":
    main()
