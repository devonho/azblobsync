# azblobsync

Environment variables used by the project.

| Variable | Required | Default | Description |
|---|:---:|---|---|
| AZURE_STORAGE_ACCOUNT_URL | Conditional | - | Account URL for Azure Storage (e.g. `https://myaccount.blob.core.windows.net`). Used as a fallback if `SOURCE_`/`TARGET_` variables are not provided. |
| AZURE_STORAGE_CONTAINER_NAME | Conditional | - | Container name used as a fallback for uploads when `SOURCE_`/`TARGET_` are not provided. |
| LOCAL_CONTAINER_PATH | Optional | - | Local filesystem path to a folder containing files to sync when running in local->container mode. When set, the tool will compare files under this path to the target container and upload missing/updated files. |
| SOURCE_AZURE_STORAGE_ACCOUNT_URL | Optional | - | Overrides `AZURE_STORAGE_ACCOUNT_URL` for the source container when doing container-to-container sync. |
| SOURCE_AZURE_STORAGE_CONTAINER_NAME | Optional | - | Overrides `AZURE_STORAGE_CONTAINER_NAME` for the source container. |
| SOURCE_AZURE_STORAGE_CONTAINER_KEY | Optional | - | Storage account key for the source account. If set the tool will prefer a named/key credential built from this value. If not set the code falls back to `ManagedIdentityCredential`. |
| TARGET_AZURE_STORAGE_ACCOUNT_URL | Optional | - | Overrides `AZURE_STORAGE_ACCOUNT_URL` for the target container. |
| TARGET_AZURE_STORAGE_CONTAINER_NAME | Optional | - | Overrides `AZURE_STORAGE_CONTAINER_NAME` for the target container. |
| TARGET_AZURE_STORAGE_CONTAINER_KEY | Optional | - | Storage account key for the target account. If set the tool will prefer a named/key credential built from this value. If not set the code falls back to `ManagedIdentityCredential`. |
| SYNC_PREFIX | Optional | - | If set, limits listing/comparison/copy to blobs whose names start with this prefix. |
| OVERWRITE_UPDATES | Optional | `true` | When `true`, blobs detected as "updates" will overwrite target blobs. When `false`, update candidates are skipped. |
| DELETE_EXTRANEOUS | Optional | `false` | When `true`, blobs that exist in the target but not in the source are deleted during sync (can also be passed programmatically). |
| METADATA_URL_BASE | Optional | - | Base URL used by `upload_files_from_list` to populate a `url` metadata entry for uploaded blobs. |
| DEBUG | Optional | `false` | When `true` enables DEBUG logging and more verbose output format. |
| LOOP_INTERVAL_MINUTES | Optional | `0` | If > 0, the sync runs repeatedly every N minutes. If 0 or unset the tool runs a single sync and exits. |
| LOOP_START_DAY_OF_WEEK | Optional | - | If set, the scheduler will wait until the next occurrence of this weekday before starting repeated runs. Accepts weekday names/abbreviations (e.g. `mon`, `monday`) or 0..6 (Mon=0). |
| LOOP_START_TIME_OF_DAY | Optional | - | If set, the scheduler will wait until this time of day (24-hour `HH:MM`) before starting the first scheduled run. |

Authentication / Credentials

The code uses Azure Identity by default (e.g. `ManagedIdentityCredential`, `DefaultAzureCredential`). If you provide the storage account key via `SOURCE_AZURE_STORAGE_CONTAINER_KEY` or `TARGET_AZURE_STORAGE_CONTAINER_KEY`, the tool will prefer a named/key credential constructed from that key for the corresponding account. If a provided key cannot be parsed into an account name or is not present, the code falls back to `ManagedIdentityCredential()`.

Ensure the identity used to run the tool has appropriate permissions on source and target storage accounts (List/Get for source; Create/Write/Delete for target as needed).

Example `.env` snippet

```
# account-level default (used if SOURCE_/TARGET_ not set)
AZURE_STORAGE_ACCOUNT_URL=https://myaccount.blob.core.windows.net
AZURE_STORAGE_CONTAINER_NAME=my-container

# local mode (optional)
LOCAL_CONTAINER_PATH=/path/to/local/folder

# explicit per-endpoint overrides (optional)
SOURCE_AZURE_STORAGE_ACCOUNT_URL=https://srcaccount.blob.core.windows.net
SOURCE_AZURE_STORAGE_CONTAINER_NAME=src-container
TARGET_AZURE_STORAGE_ACCOUNT_URL=https://tgtaccount.blob.core.windows.net
TARGET_AZURE_STORAGE_CONTAINER_NAME=tgt-container

# optional storage account keys (if you want key-based auth instead of managed identity)
SOURCE_AZURE_STORAGE_CONTAINER_KEY=VBh...your_source_account_key...
TARGET_AZURE_STORAGE_CONTAINER_KEY=Q2F...your_target_account_key...

# behavior
SYNC_PREFIX=some/path/
OVERWRITE_UPDATES=true
DELETE_EXTRANEOUS=false
METADATA_URL_BASE=https://example.com/metadata
DEBUG=false

# scheduler (optional)
LOOP_INTERVAL_MINUTES=15
LOOP_START_DAY_OF_WEEK=mon
LOOP_START_TIME_OF_DAY=02:30
```

## Running in Azure Container Apps (RBAC)

If you deploy this tool in Azure Container Apps and use the System Assigned Managed Identity provided to the Container App, assign these built-in Azure RBAC roles to that identity so it can pull the container image and access blobs:

- AcrPull — grant on the Azure Container Registry resource (allows the Container App to pull the container image).
- Storage Blob Data Reader — grant on the source storage account or container (read/list permissions for source blobs).
- Storage Blob Data Contributor — grant on the target storage account or container (create/write/delete permissions for target blobs).

Optional/alternative:
- Storage Blob Data Owner — grants full data-plane control; use only if broader privileges are required.
- Container Apps Contributor — grant on the deploying VM's system-assigned MSI to enable streaming of logs.

Notes and best practices:
- Grant the roles at the smallest practical scope (prefer container-level or storage-account-level rather than subscription-level).
- If you prefer not to use Managed Identity, you can supply account keys (via `SOURCE_AZURE_STORAGE_CONTAINER_KEY` / `TARGET_AZURE_STORAGE_CONTAINER_KEY`) or a SAS token; those options do not require RBAC assignments but require secure secret handling.
- To assign a role via CLI, use a command like:

```
az role assignment create --assignee <principalId> --role AcrPull --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.ContainerRegistry/registries/<acrName>
```

Replace `AcrPull` with the appropriate role and change the `--scope` to the storage account or container resource as needed.

Notes

- The main sync wrapper is `blob_container_source_blob_container_target_main()` in `src/main.py`. It reads environment variables (or accepts programmatic overrides) and performs compare/copy/delete actions.
- For uploads from local filesystem use `local_source_blob_container_target()` in `src/main.py`, which expects a base path as a command-line argument and reads `filenames.txt` for the list of files to upload.