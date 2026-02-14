# azblobsync

**azblobsync** is a Python-based one-way synchronization tool for Azure Blob Storage that intelligently compares and syncs files between sources and targets. It supports two primary sync modes:

1. **Local-to-Container**: Sync files from a local filesystem directory to an Azure Blob Storage container
2. **Container-to-Container**: Sync blobs between two Azure Blob Storage containers (even across different storage accounts)

Key Features

- **Smart Comparison**: Compares files/blobs based on size and modification time to identify what needs to be created, updated, or deleted
- **Incremental Sync**: Only transfers files that have changed, minimizing data transfer and costs
- **Flexible Control**: Configure whether to skip updates, skip new file creation, or skip deletions
- **Prefix Filtering**: Limit sync operations to specific paths using prefix filters
- **Multiple Authentication Methods**: Supports Managed Identity, Azure CLI credentials, and storage account keys
- **Scheduled Execution**: Built-in scheduler for automated periodic syncs with configurable intervals and start times

Use Cases

- Backup and disaster recovery between storage accounts
- Content distribution and replication across regions
- Deployment of static assets from local builds to cloud storage
- Scheduled data synchronization for data pipelines
- One-way mirroring of blob containers

## General environment variables

| Variable | Required | Default | Description |
|---|:---:|---|---|
| DEBUG | Optional | `false` | When `true` enables DEBUG logging and more verbose output format. |
| METADATA_URL_BASE | Optional | - | Base URL used to populate a `url` blob metadata entry for uploaded blobs. |
| SYNC_PREFIX | Optional | - | If set, limits listing/comparison/copy to blobs whose names start with this prefix. |

## Source and target environment variables

| Variable | Required | Default | Description |
|---|:---:|---|---|
| SOURCE_LOCAL_CONTAINER_PATH | Optional | - | Local filesystem path to a folder containing files to sync when running in local->container mode. When set, the tool will compare files under this path to the target container and upload missing/updated files. |
| SOURCE_AZURE_STORAGE_ACCOUNT_URL | Required | - | Account URL for the source storage account when doing container-to-container sync (e.g. `https://srcaccount.blob.core.windows.net`). |
| SOURCE_AZURE_STORAGE_CONTAINER_NAME | Required | - | Source container name for container-to-container sync. |
| SOURCE_AZURE_STORAGE_CONTAINER_KEY | Optional | - | Storage account key for the source account. If set the tool will prefer a named/key credential built from this value. If not set the code falls back to `ManagedIdentityCredential`. |
| TARGET_AZURE_STORAGE_ACCOUNT_URL | Required | - | Account URL for the target storage account (e.g. `https://tgtaccount.blob.core.windows.net`). |
| TARGET_AZURE_STORAGE_CONTAINER_NAME | Required | - | Target container name. |
| TARGET_AZURE_STORAGE_CONTAINER_KEY | Optional | - | Storage account key for the target account. If set the tool will prefer a named/key credential built from this value. If not set the code falls back to `ManagedIdentityCredential`. |

## Sync options environment variables

| Variable | Required | Default | Description |
|---|:---:|---|---|
| SKIP_UPDATES | Optional | `true` | When `true`, blobs detected as "updates" will overwrite target blobs. When `false`, update candidates are skipped. |
| SKIP_DELETE | Optional | `true` | When `true`, blobs that exist in the target but not in the source are deleted during sync (can also be passed programmatically). |
| SKIP_COPY | Optional | `true` | When set to `true`, the sync will skip uploading any files detected as "create" candidates (files present locally but missing in the target). Useful to preview or avoid initial bulk uploads. |

## Scheduler environment variables

| Variable | Required | Default | Description |
|---|:---:|---|---|
| LOOP_INTERVAL_MINUTES | Optional | `0` | If > 0, the sync runs repeatedly every N minutes. If 0 or unset the tool runs a single sync and exits. |
| LOOP_START_DAY_OF_WEEK | Optional | - | If set, the scheduler will wait until the next occurrence of this weekday before starting repeated runs. Accepts weekday names/abbreviations (e.g. `mon`, `monday`) or 0..6 (Mon=0). |
| LOOP_START_TIME_OF_DAY | Optional | - | If set, the scheduler will wait until this time of day (24-hour `HH:MM`) before starting the first scheduled run. |

## Authentication / Credentials

The code uses Azure Identity by default (e.g. `ManagedIdentityCredential`). If you provide the storage account key via `SOURCE_AZURE_STORAGE_CONTAINER_KEY` or `TARGET_AZURE_STORAGE_CONTAINER_KEY`, the tool will prefer a named/key credential constructed from that key for the corresponding account. If a provided key cannot be parsed into an account name or is not present, the code falls back to `ManagedIdentityCredential()`.

Ensure the identity used to run the tool has appropriate permissions on source and target storage accounts (List/Get for source; Create/Write/Delete for target as needed).

Example `.env` snippet

```
# local mode (optional)
SOURCE_LOCAL_CONTAINER_PATH=/path/to/local/folder
# if you want to avoid uploading newly discovered files, set SKIP_COPY=true
SKIP_COPY=false

# container-to-container sync mode
SOURCE_AZURE_STORAGE_ACCOUNT_URL=https://srcaccount.blob.core.windows.net
SOURCE_AZURE_STORAGE_CONTAINER_NAME=src-container
TARGET_AZURE_STORAGE_ACCOUNT_URL=https://tgtaccount.blob.core.windows.net
TARGET_AZURE_STORAGE_CONTAINER_NAME=tgt-container

# optional storage account keys (if you want key-based auth instead of managed identity)
SOURCE_AZURE_STORAGE_CONTAINER_KEY=VBh...your_source_account_key...
TARGET_AZURE_STORAGE_CONTAINER_KEY=Q2F...your_target_account_key...

# behavior
SYNC_PREFIX=some/path/
SKIP_UPDATES=true
SKIP_DELETE=false
METADATA_URL_BASE=https://example.com/metadata
DEBUG=false
SKIP_COPY=false

# scheduler (optional)
LOOP_INTERVAL_MINUTES=15
LOOP_START_DAY_OF_WEEK=mon
LOOP_START_TIME_OF_DAY=02:30
```

## Running in Azure Container Apps (RBAC)

If you deploy this tool in Azure Container Apps and use the System Assigned Managed Identity provided to the Container App, assign these built-in Azure RBAC roles to that identity so it can pull the container image and access blobs:

- AcrPull — grant on the Azure Container Registry resource (allows the Container App to pull the container image).
- Storage Blob Data Reader — grant on the **source** storage account or container (read/list permissions for source blobs).
- Storage Blob Data Contributor — grant on the **target** storage account or container (create/write/delete permissions for target blobs).

Optional/alternative:
- Storage Blob Data Owner — grants full data-plane control; use only if broader privileges are required.
- Container Apps Contributor — grant on the deploying VM's system-assigned MSI to enable streaming of logs.

Notes:
- If you prefer not to use Managed Identity, you can supply account keys (via `SOURCE_AZURE_STORAGE_CONTAINER_KEY` / `TARGET_AZURE_STORAGE_CONTAINER_KEY`) or a SAS token; those options do not require RBAC assignments but require secure secret handling.
