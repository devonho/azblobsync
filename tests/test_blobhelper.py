"""
Integration tests for blobhelper module using actual Azure Blob Storage API.

These tests require valid Azure credentials and will create/delete test containers.
Ensure the following environment variables are set:
- AZURE_STORAGE_ACCOUNT_URL (or TEST_AZURE_STORAGE_ACCOUNT_URL)
- AZURE_STORAGE_CONTAINER_NAME (optional, defaults to 'test-azblobsync')

Tests use DefaultAzureCredential which tries multiple authentication methods.
"""
import os
import unittest
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Import functions to test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from blobhelper import (
    get_blob_service_client,
    get_container_client,
    create_folder_structure,
    create_folder_from_path,
    create_folders_from_list,
    upload_files_from_list,
    compare_containers,
    copy_blobs,
    remove_placeholder_files,
)


class TestBlobHelper(unittest.TestCase):
    """Integration tests for blobhelper functions using actual Azure API."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment - runs once before all tests."""
        # Get account URL from environment
        cls.account_url = os.getenv("TEST_AZURE_STORAGE_ACCOUNT_URL") or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        if not cls.account_url:
            raise ValueError("AZURE_STORAGE_ACCOUNT_URL or TEST_AZURE_STORAGE_ACCOUNT_URL must be set")

        # Use test container
        cls.test_container = os.getenv("TEST_AZURE_STORAGE_CONTAINER_NAME", "test-azblobsync")
        cls.test_container_source = f"{cls.test_container}-source"
        cls.test_container_target = f"{cls.test_container}-target"
        
        cls.credential = DefaultAzureCredential()
        cls.blob_service_client = BlobServiceClient(account_url=cls.account_url, credential=cls.credential)
        
        # Create test containers
        for container_name in [cls.test_container, cls.test_container_source, cls.test_container_target]:
            try:
                cls.blob_service_client.create_container(container_name)
                print(f"Created test container: {container_name}")
            except Exception as e:
                print(f"Container {container_name} may already exist or error: {e}")

        # Create temporary directory for local file tests
        cls.temp_dir = tempfile.mkdtemp(prefix="azblobsync_test_")
        print(f"Created temporary directory: {cls.temp_dir}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment - runs once after all tests."""
        # Delete test containers
        for container_name in [cls.test_container, cls.test_container_source, cls.test_container_target]:
            try:
                #cls.blob_service_client.delete_container(container_name)
                print(f"Deleted test container: {container_name}")
            except Exception as e:
                print(f"Error deleting container {container_name}: {e}")

        # Clean up temp directory
        import shutil
        try:
            shutil.rmtree(cls.temp_dir)
            print(f"Deleted temporary directory: {cls.temp_dir}")
        except Exception as e:
            print(f"Error deleting temp directory: {e}")

    def setUp(self):
        """Clean up test container before each test."""
        self._cleanup_container(self.test_container)
        self._cleanup_container(self.test_container_source)
        self._cleanup_container(self.test_container_target)

    def _cleanup_container(self, container_name):
        """Delete all blobs from a container."""
        # Ensure container exists
        try:
            self.blob_service_client.create_container(container_name)
        except Exception:
            # Container already exists, which is fine
            pass
        
        # Wait for container to be available
        container_client = self.blob_service_client.get_container_client(container_name)
        max_retries = 30
        retry_count = 0
        while retry_count < max_retries:
            try:
                container_client.get_container_properties()
                break
            except Exception:
                time.sleep(0.5)
                retry_count += 1
        
        # Clean container
        try:
            blobs = list(container_client.list_blobs())
            for blob in blobs:
                container_client.delete_blob(blob.name)
        except Exception as e:
            print(f"Warning: Error cleaning container {container_name}: {e}")

    def test_get_blob_service_client(self):
        """Test getting a BlobServiceClient."""
        client = get_blob_service_client(self.account_url, self.credential)
        self.assertIsNotNone(client)
        self.assertIsInstance(client, BlobServiceClient)
        
        # Test with no credential (should use DefaultAzureCredential)
        client2 = get_blob_service_client(self.account_url)
        self.assertIsNotNone(client2)

    def test_get_container_client(self):
        """Test getting a ContainerClient."""
        container_client = get_container_client(self.account_url, self.test_container, self.credential)
        self.assertIsNotNone(container_client)
        
        # Verify it can perform operations
        props = container_client.get_container_properties()
        self.assertIsNotNone(props)

    def test_create_folder_structure(self):
        """Test creating folder structure with placeholder blobs."""
        folder_paths = [
            "level1",
            "level1/level2",
            "level1/level2/level3",
            "another_folder",
        ]
        
        create_folder_structure(
            self.account_url,
            self.test_container,
            folder_paths,
            self.credential
        )
        
        # Verify placeholder blobs were created
        container_client = self.blob_service_client.get_container_client(self.test_container)
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        expected_placeholders = [
            "level1/.placeholder",
            "level1/level2/.placeholder",
            "level1/level2/level3/.placeholder",
            "another_folder/.placeholder",
        ]
        
        for placeholder in expected_placeholders:
            self.assertIn(placeholder, blob_names, f"Expected placeholder {placeholder} not found")

    def test_create_folder_from_path(self):
        """Test creating a single folder path with all parents."""
        path = "parent/child/grandchild"
        
        create_folder_from_path(
            self.account_url,
            self.test_container,
            path,
            self.credential
        )
        
        # Verify all parent placeholders were created
        container_client = self.blob_service_client.get_container_client(self.test_container)
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        expected_placeholders = [
            "parent/.placeholder",
            "parent/child/.placeholder",
            "parent/child/grandchild/.placeholder",
        ]
        
        for placeholder in expected_placeholders:
            self.assertIn(placeholder, blob_names)

    def test_create_folders_from_list(self):
        """Test creating folders from a list of dictionaries."""
        folder_list = [
            {"path": "folder1", "level": 0},
            {"path": "folder1/subfolder", "level": 1},
            {"path": "folder2", "level": 0},
        ]
        
        create_folders_from_list(
            self.account_url,
            self.test_container,
            folder_list,
            self.credential
        )
        
        container_client = self.blob_service_client.get_container_client(self.test_container)
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        self.assertIn("folder1/.placeholder", blob_names)
        self.assertIn("folder1/subfolder/.placeholder", blob_names)
        self.assertIn("folder2/.placeholder", blob_names)

    def test_upload_files_from_list(self):
        """Test uploading multiple files from local paths."""
        # Create test files
        test_files = []
        for i in range(3):
            file_path = Path(self.temp_dir) / f"test_file_{i}.txt"
            file_path.write_text(f"Test content {i}")
            test_files.append(str(file_path))
        
        # Upload files
        upload_files_from_list(
            self.account_url,
            self.test_container,
            test_files,
            base_path=self.temp_dir,
            credential=self.credential,
            overwrite=True
        )
        
        # Verify files were uploaded
        container_client = self.blob_service_client.get_container_client(self.test_container)
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        for i in range(3):
            self.assertIn(f"test_file_{i}.txt", blob_names)
        
        # Verify content
        blob_client = container_client.get_blob_client("test_file_0.txt")
        content = blob_client.download_blob().readall()
        self.assertEqual(content.decode(), "Test content 0")

    def test_upload_files_with_subdirectories(self):
        """Test uploading files maintaining directory structure."""
        # Create nested directory structure
        subdir = Path(self.temp_dir) / "subdir1" / "subdir2"
        subdir.mkdir(parents=True)
        
        file1 = Path(self.temp_dir) / "root_file.txt"
        file2 = subdir / "nested_file.txt"
        
        file1.write_text("Root content")
        file2.write_text("Nested content")
        
        upload_files_from_list(
            self.account_url,
            self.test_container,
            [str(file1), str(file2)],
            base_path=self.temp_dir,
            credential=self.credential
        )
        
        # Verify structure
        container_client = self.blob_service_client.get_container_client(self.test_container)
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        self.assertIn("root_file.txt", blob_names)
        self.assertIn("subdir1/subdir2/nested_file.txt", blob_names)

    def test_compare_containers_empty(self):
        """Test comparing two empty containers."""
        result = compare_containers(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            self.credential,
            self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 0)
        self.assertEqual(result["summary"]["update"], 0)
        self.assertEqual(result["summary"]["delete"], 0)

    def test_compare_containers_create(self):
        """Test comparing containers when source has blobs that target doesn't."""
        # Add blobs to source
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        source_client.upload_blob("file1.txt", b"content1")
        source_client.upload_blob("file2.txt", b"content2")
        
        # Compare
        result = compare_containers(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            self.credential,
            self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 2)
        self.assertIn("file1.txt", result["to_create"])
        self.assertIn("file2.txt", result["to_create"])

    def test_compare_containers_delete(self):
        """Test comparing containers when target has blobs that source doesn't."""
        # Add blobs to target only
        target_client = self.blob_service_client.get_container_client(self.test_container_target)
        target_client.upload_blob("old_file.txt", b"old content")
        
        result = compare_containers(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            self.credential,
            self.credential
        )
        
        self.assertEqual(result["summary"]["delete"], 1)
        self.assertIn("old_file.txt", result["to_delete"])

    def test_compare_containers_update(self):
        """Test comparing containers when blobs need updating based on timestamp."""
        # Add same blob to both containers with different timestamps
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        target_client = self.blob_service_client.get_container_client(self.test_container_target)
        
        # Upload to target first (older)
        target_client.upload_blob("file.txt", b"old content")
        
        # Wait a moment to ensure different timestamps
        time.sleep(2)
        
        # Upload to source (newer)
        source_client.upload_blob("file.txt", b"new content")
        
        result = compare_containers(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            self.credential,
            self.credential
        )
        
        self.assertEqual(result["summary"]["update"], 1)
        self.assertIn("file.txt", result["to_update"])

    def test_compare_containers_with_prefix(self):
        """Test comparing containers with a specific prefix filter."""
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        source_client.upload_blob("docs/file1.txt", b"content1")
        source_client.upload_blob("docs/file2.txt", b"content2")
        source_client.upload_blob("other/file3.txt", b"content3")
        
        result = compare_containers(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            self.credential,
            self.credential,
            prefix="docs/"
        )
        
        # Should only see files under docs/ prefix
        self.assertEqual(result["summary"]["create"], 2)
        self.assertIn("docs/file1.txt", result["to_create"])
        self.assertIn("docs/file2.txt", result["to_create"])

    def test_copy_blobs_basic(self):
        """Test copying blobs from source to target container."""
        # Setup source blobs
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        source_client.upload_blob("file1.txt", b"content1")
        source_client.upload_blob("folder/file2.txt", b"content2")
        
        # Copy blobs
        result = copy_blobs(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            ["file1.txt", "folder/file2.txt"],
            self.credential,
            self.credential,
            overwrite=True
        )
        
        self.assertEqual(result["summary"]["copied"], 2)
        self.assertEqual(result["summary"]["errors"], 0)
        
        # Verify blobs exist in target
        target_client = self.blob_service_client.get_container_client(self.test_container_target)
        blob1 = target_client.download_blob("file1.txt").readall()
        blob2 = target_client.download_blob("folder/file2.txt").readall()
        
        self.assertEqual(blob1, b"content1")
        self.assertEqual(blob2, b"content2")

    def test_copy_blobs_with_folders(self):
        """Test copying blobs with folder placeholder creation."""
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        source_client.upload_blob("deep/nested/folder/file.txt", b"content")
        
        result = copy_blobs(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            ["deep/nested/folder/file.txt"],
            self.credential,
            self.credential,
            create_folders=True,
            overwrite=True
        )
        
        self.assertEqual(result["summary"]["copied"], 1)
        
        # Check that folder placeholders were created
        target_client = self.blob_service_client.get_container_client(self.test_container_target)
        blobs = list(target_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        # Should have the file and possibly parent folder placeholders
        self.assertIn("deep/nested/folder/file.txt", blob_names)

    def test_copy_blobs_skip_existing(self):
        """Test copying blobs with overwrite=False skips existing blobs."""
        # Setup
        source_client = self.blob_service_client.get_container_client(self.test_container_source)
        target_client = self.blob_service_client.get_container_client(self.test_container_target)
        
        source_client.upload_blob("file.txt", b"new content")
        target_client.upload_blob("file.txt", b"old content")
        
        result = copy_blobs(
            self.account_url,
            self.test_container_source,
            self.account_url,
            self.test_container_target,
            ["file.txt"],
            self.credential,
            self.credential,
            overwrite=False
        )
        
        self.assertEqual(result["summary"]["skipped"], 1)
        self.assertEqual(result["summary"]["copied"], 0)
        
        # Verify old content is still there
        content = target_client.download_blob("file.txt").readall()
        self.assertEqual(content, b"old content")

    def test_remove_placeholder_files(self):
        """Test removing placeholder files from container."""
        # Create folders with placeholders
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("folder1/.placeholder", b"")
        container_client.upload_blob("folder2/.placeholder", b"")
        container_client.upload_blob("folder3/subfolder/.placeholder", b"")
        container_client.upload_blob("regular_file.txt", b"content")
        
        # Remove placeholders
        result = remove_placeholder_files(
            self.account_url,
            self.test_container,
            self.credential
        )
        
        self.assertEqual(result["summary"]["removed"], 3)
        
        # Verify only regular file remains
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        self.assertEqual(len(blob_names), 1)
        self.assertIn("regular_file.txt", blob_names)

    def test_remove_placeholder_files_dry_run(self):
        """Test dry run mode for removing placeholder files."""
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("folder/.placeholder", b"")
        
        result = remove_placeholder_files(
            self.account_url,
            self.test_container,
            self.credential,
            dry_run=True
        )
        
        self.assertEqual(result["summary"]["removed"], 1)
        
        # Verify placeholder still exists (dry run didn't delete)
        blobs = list(container_client.list_blobs())
        self.assertEqual(len(blobs), 1)

    def test_remove_placeholder_files_with_prefix(self):
        """Test removing placeholder files with prefix filter."""
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("docs/folder1/.placeholder", b"")
        container_client.upload_blob("docs/folder2/.placeholder", b"")
        container_client.upload_blob("other/folder3/.placeholder", b"")
        
        result = remove_placeholder_files(
            self.account_url,
            self.test_container,
            self.credential,
            prefix="docs/"
        )
        
        self.assertEqual(result["summary"]["removed"], 2)
        
        # Verify only the "other" placeholder remains
        blobs = list(container_client.list_blobs())
        blob_names = [blob.name for blob in blobs]
        
        self.assertEqual(len(blob_names), 1)
        self.assertIn("other/folder3/.placeholder", blob_names)


if __name__ == "__main__":
    unittest.main()
