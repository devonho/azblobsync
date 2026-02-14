# Azure Blob Sync - Test Suite

This directory contains integration tests for the Azure Blob Sync project. These tests use **actual Azure Blob Storage APIs** (no mocking) to ensure the code works correctly with real Azure resources.

## Prerequisites

1. **Azure Storage Account**: You need access to an Azure Storage Account with permissions to create/delete containers and blobs.

2. **Azure Authentication**: Tests use `DefaultAzureCredential` which tries multiple authentication methods in order:
   - Environment variables
   - Managed Identity
   - Azure CLI (`az login`)
   - Visual Studio Code
   - Azure PowerShell

   The easiest way is to authenticate via Azure CLI:
   ```bash
   az login
   ```

3. **Python Dependencies**: Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Set the following environment variables before running tests:

### Required
- `AZURE_STORAGE_ACCOUNT_URL` or `TEST_AZURE_STORAGE_ACCOUNT_URL`: Your Azure Storage account URL
  ```bash
  # Example:
  export AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"
  ```

### Optional
- `TEST_AZURE_STORAGE_CONTAINER_NAME`: Name for test containers (default: `test-azblobsync`)
  - The tests will create containers with this name plus suffixes like `-source`, `-target`
  - Containers are automatically created and cleaned up

## Running Tests

### List Available Tests
```bash
# List all tests without running them
pytest tests/ --collect-only

# List with more detail
pytest tests/ --collect-only -v
```

### Run All Tests
```bash
# Using pytest (recommended)
pytest tests/ -v

# Using unittest
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run Specific Test Files
```bash
# Test blob helper functions
pytest tests/test_blobhelper.py -v

# Test local filesystem functions
pytest tests/test_localfshelper.py -v
```

### Run Specific Test Cases
```bash
# Run tests matching a pattern
pytest tests/ -k "folder" -v

# Run a specific test class
pytest tests/test_blobhelper.py::TestBlobHelper -v

# Run a specific test method
pytest tests/test_blobhelper.py::TestBlobHelper::test_create_folder_structure -v

# Show print output
pytest tests/ -v -s

# Stop on first failure
pytest tests/ -x

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Test Coverage

### test_blobhelper.py
Tests for `src/blobhelper.py` functions:
- ✅ `get_blob_service_client()` - Create blob service clients
- ✅ `get_container_client()` - Get container clients
- ✅ `create_folder_structure()` - Create folder hierarchies with placeholders
- ✅ `create_folder_from_path()` - Create single folder path with parents
- ✅ `create_folders_from_list()` - Create folders from list of dicts
- ✅ `upload_files_from_list()` - Upload multiple files preserving structure
- ✅ `compare_containers()` - Compare source and target containers
- ✅ `copy_blobs()` - Copy blobs between containers
- ✅ `remove_placeholder_files()` - Remove folder placeholder blobs

### test_localfshelper.py
Tests for `src/localfshelper.py` functions:
- ✅ `compare_local_to_container()` - Compare local filesystem to Azure container
  - Create operations (local files not in container)
  - Update operations (local files newer than blobs)
  - Delete operations (blobs not in local)
  - Prefix filtering
  - Nested directory structures
  - Special characters in filenames
  - Placeholder blob handling

## Test Behavior

### Container Management
- Tests create temporary containers with names like `test-azblobsync`, `test-azblobsync-source`, `test-azblobsync-target`
- Containers are **automatically created** before tests run
- Containers are **automatically deleted** after all tests complete
- Each test method cleans up blobs before running to ensure isolation

### Local Files
- Tests create temporary directories for local file operations
- Temporary directories are automatically cleaned up after tests
- Located in system temp directory with prefix `azblobsync_test_` or `localfs_test_`

### Safety
- Tests are designed to be safe and idempotent
- All Azure resources are created with test-specific names
- Cleanup happens automatically even if tests fail
- No production data should be affected

## Troubleshooting

### Quick Start
```bash
# 1. Authenticate with Azure
az login

# 2. Set environment variable (PowerShell)
$env:AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"

# 3. Run tests
pytest tests/ -v
```

### Authentication Errors
```
Error: DefaultAzureCredential failed to retrieve a token
```
**Solution**: Run `az login` to authenticate with Azure CLI

### Permission Errors
```
Error: This request is not authorized to perform this operation
```
**Solution**: Ensure your Azure account has permissions to create containers and upload blobs in the storage account

### Container Already Exists
```
Error: Container already exists
```
**Solution**: Tests handle this gracefully. If containers persist, you can delete them manually:
```bash
az storage container delete --name test-azblobsync --account-name youraccount
```

### Environment Variable Not Set
```
ValueError: AZURE_STORAGE_ACCOUNT_URL or TEST_AZURE_STORAGE_ACCOUNT_URL must be set
```
**Solution**: Set the required environment variable with your storage account URL

## Adding New Tests

When adding new test cases:

1. **Setup and Cleanup**: Use `setUp()` for per-test initialization and `setUpClass()` for one-time setup
2. **Container Cleanup**: Clean containers in `setUp()` to ensure test isolation
3. **Assertions**: Use descriptive assertion messages
4. **Documentation**: Add docstrings explaining what each test validates
5. **Independence**: Tests should not depend on each other's state

Example test structure:
```python
def test_your_feature(self):
    """Test description explaining what this validates."""
    # Arrange - set up test data
    # Act - perform the operation
    # Assert - verify the results
    self.assertEqual(actual, expected, "Descriptive message")
```

## Continuous Integration

These tests can be integrated into CI/CD pipelines. Ensure the CI environment has:
- Azure credentials configured (Service Principal or Managed Identity)
- Environment variables set
- Network access to Azure Storage

Example GitHub Actions:
```yaml
- name: Run Azure Integration Tests
  env:
    AZURE_STORAGE_ACCOUNT_URL: ${{ secrets.AZURE_STORAGE_ACCOUNT_URL }}
    AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
    AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
    AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
  run: |
    pytest tests/ -v
```

## Cost Considerations

Running these tests will incur minimal Azure costs:
- Storage transactions (PUT, GET, DELETE operations)
- Storage capacity (temporary, cleaned up automatically)
- Costs are typically < $0.01 per test run

For high-frequency testing, consider using:
- Azure Storage Emulator (Azurite) for local development
- Dedicated test storage accounts with lifecycle policies
- Test quotas to limit resource usage
