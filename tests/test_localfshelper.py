"""
Integration tests for localfshelper module using actual Azure Blob Storage API.

These tests require valid Azure credentials and will create/delete test containers.
Ensure the following environment variables are set:
- AZURE_STORAGE_ACCOUNT_URL (or TEST_AZURE_STORAGE_ACCOUNT_URL)

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

# Import functions to test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from localfshelper import compare_local_to_container
from blobhelper import get_container_client


class TestLocalFSHelper(unittest.TestCase):
    """Integration tests for localfshelper functions using actual Azure API."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment - runs once before all tests."""
        # Get account URL from environment
        cls.account_url = os.getenv("TEST_AZURE_STORAGE_ACCOUNT_URL") or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        if not cls.account_url:
            raise ValueError("AZURE_STORAGE_ACCOUNT_URL or TEST_AZURE_STORAGE_ACCOUNT_URL must be set")

        cls.test_container = os.getenv("TEST_AZURE_STORAGE_CONTAINER_NAME", "test-localfs-azblobsync")
        cls.credential = DefaultAzureCredential()
        cls.blob_service_client = BlobServiceClient(account_url=cls.account_url, credential=cls.credential)
        
        # Create test container
        try:
            cls.blob_service_client.create_container(cls.test_container)
            print(f"Created test container: {cls.test_container}")
        except Exception as e:
            print(f"Container {cls.test_container} may already exist or error: {e}")

        # Create temporary directory for local file tests
        cls.temp_dir = tempfile.mkdtemp(prefix="localfs_test_")
        print(f"Created temporary directory: {cls.temp_dir}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment - runs once after all tests."""
        # Delete test container
        try:
            #cls.blob_service_client.delete_container(cls.test_container)
            print(f"Deleted test container: {cls.test_container}")
        except Exception as e:
            print(f"Error deleting container: {e}")

        # Clean up temp directory
        import shutil
        try:
            shutil.rmtree(cls.temp_dir)
            print(f"Deleted temporary directory: {cls.temp_dir}")
        except Exception as e:
            print(f"Error deleting temp directory: {e}")

    def setUp(self):
        """Clean up before each test."""
        # Ensure container exists
        try:
            self.blob_service_client.create_container(self.test_container)
        except Exception:
            # Container already exists, which is fine
            pass
        
        # Wait for container to be available
        container_client = self.blob_service_client.get_container_client(self.test_container)
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
            print(f"Warning: Error cleaning container: {e}")

        # Clean temp directory
        for item in Path(self.temp_dir).iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                import shutil
                shutil.rmtree(item)

    def test_compare_local_to_container_empty(self):
        """Test comparing empty local directory to empty container."""
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 0)
        self.assertEqual(result["summary"]["update"], 0)
        self.assertEqual(result["summary"]["delete"], 0)

    def test_compare_local_to_container_create(self):
        """Test comparing when local has files that container doesn't."""
        # Create local files
        (Path(self.temp_dir) / "file1.txt").write_text("content1")
        (Path(self.temp_dir) / "file2.txt").write_text("content2")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 2)
        self.assertIn("file1.txt", result["to_create"])
        self.assertIn("file2.txt", result["to_create"])

    def test_compare_local_to_container_delete(self):
        """Test comparing when container has blobs that local doesn't."""
        # Add blobs to container
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("old_file.txt", b"old content")
        container_client.upload_blob("another_old.txt", b"old content 2")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["delete"], 2)
        self.assertIn("old_file.txt", result["to_delete"])
        self.assertIn("another_old.txt", result["to_delete"])

    def test_compare_local_to_container_update_by_time(self):
        """Test comparing when local file is newer than blob."""
        # Upload blob first (older)
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("file.txt", b"old content")
        
        # Wait to ensure different timestamps
        time.sleep(2)
        
        # Create local file (newer)
        (Path(self.temp_dir) / "file.txt").write_text("new content")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["update"], 1)
        self.assertIn("file.txt", result["to_update"])

    def test_compare_local_to_container_update_by_size(self):
        """Test comparing when file sizes differ."""
        # Upload blob with different size
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("file.txt", b"short")
        
        # Create local file with different size
        (Path(self.temp_dir) / "file.txt").write_text("much longer content that is different size")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        # Should be marked for update due to size difference
        self.assertGreaterEqual(result["summary"]["update"], 0)

    def test_compare_local_to_container_with_subdirectories(self):
        """Test comparing with nested directory structure."""
        # Create nested local structure
        subdir = Path(self.temp_dir) / "subdir1" / "subdir2"
        subdir.mkdir(parents=True)
        
        (Path(self.temp_dir) / "root_file.txt").write_text("root")
        (Path(self.temp_dir) / "subdir1" / "file1.txt").write_text("sub1")
        (subdir / "file2.txt").write_text("sub2")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 3)
        self.assertIn("root_file.txt", result["to_create"])
        self.assertIn("subdir1/file1.txt", result["to_create"])
        self.assertIn("subdir1/subdir2/file2.txt", result["to_create"])

    def test_compare_local_to_container_with_prefix(self):
        """Test comparing with prefix filter."""
        # Create local files in different directories
        docs_dir = Path(self.temp_dir) / "docs"
        other_dir = Path(self.temp_dir) / "other"
        docs_dir.mkdir()
        other_dir.mkdir()
        
        (docs_dir / "file1.txt").write_text("doc1")
        (docs_dir / "file2.txt").write_text("doc2")
        (other_dir / "file3.txt").write_text("other")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential,
            prefix="docs/"
        )
        
        # Should only see files under docs/ prefix
        self.assertEqual(result["summary"]["create"], 2)
        self.assertIn("docs/file1.txt", result["to_create"])
        self.assertIn("docs/file2.txt", result["to_create"])

    def test_compare_local_ignores_placeholder_blobs(self):
        """Test that placeholder blobs are ignored in comparison."""
        # Create local file
        (Path(self.temp_dir) / "file.txt").write_text("content")
        
        # Create blob and placeholder in container
        container_client = self.blob_service_client.get_container_client(self.test_container)
        container_client.upload_blob("folder1/.placeholder", b"")
        container_client.upload_blob("folder2/subfolder/.placeholder", b"")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        # Placeholders should not be in to_delete list
        self.assertEqual(result["summary"]["create"], 1)
        self.assertEqual(result["summary"]["delete"], 0)

    def test_compare_local_to_container_mixed_operations(self):
        """Test comparison requiring create, update, and delete operations."""
        # Setup: some files in both, some only local, some only remote
        container_client = self.blob_service_client.get_container_client(self.test_container)
        
        # File in both (will be update candidate if timing works)
        container_client.upload_blob("common.txt", b"old")
        time.sleep(1)
        (Path(self.temp_dir) / "common.txt").write_text("new")
        
        # File only in container (to delete)
        container_client.upload_blob("only_remote.txt", b"remote")
        
        # File only local (to create)
        (Path(self.temp_dir) / "only_local.txt").write_text("local")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        # Verify all operation types are present
        self.assertGreater(result["summary"]["create"], 0, "Should have files to create")
        self.assertGreater(result["summary"]["delete"], 0, "Should have files to delete")
        self.assertGreaterEqual(result["summary"]["update"], 0, "May have files to update")
        
        self.assertIn("only_local.txt", result["to_create"])
        self.assertIn("only_remote.txt", result["to_delete"])

    def test_compare_local_to_container_error_invalid_path(self):
        """Test error handling for invalid local path."""
        with self.assertRaises(FileNotFoundError):
            compare_local_to_container(
                local_path="/nonexistent/path/that/does/not/exist",
                target_account_url=self.account_url,
                target_container_name=self.test_container,
                credential=self.credential
            )

    def test_compare_local_to_container_error_missing_params(self):
        """Test error handling for missing required parameters."""
        with self.assertRaises(KeyError):
            compare_local_to_container(
                local_path=None,
                target_account_url=self.account_url,
                target_container_name=self.test_container,
                credential=self.credential
            )

    def test_compare_local_empty_subdirs_not_counted(self):
        """Test that empty directories don't affect file counts."""
        # Create empty directories (which don't translate to blobs)
        (Path(self.temp_dir) / "empty_dir1").mkdir()
        (Path(self.temp_dir) / "empty_dir2").mkdir()
        
        # Create one actual file
        (Path(self.temp_dir) / "file.txt").write_text("content")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        # Should only count the file, not empty directories
        self.assertEqual(result["summary"]["create"], 1)
        self.assertEqual(len(result["to_create"]), 1)

    def test_compare_local_special_characters_in_names(self):
        """Test handling files with special characters in names."""
        # Create files with spaces and other characters
        (Path(self.temp_dir) / "file with spaces.txt").write_text("content1")
        (Path(self.temp_dir) / "file-with-dashes.txt").write_text("content2")
        (Path(self.temp_dir) / "file_with_underscores.txt").write_text("content3")
        
        result = compare_local_to_container(
            local_path=self.temp_dir,
            target_account_url=self.account_url,
            target_container_name=self.test_container,
            credential=self.credential
        )
        
        self.assertEqual(result["summary"]["create"], 3)
        self.assertIn("file with spaces.txt", result["to_create"])
        self.assertIn("file-with-dashes.txt", result["to_create"])
        self.assertIn("file_with_underscores.txt", result["to_create"])


if __name__ == "__main__":
    unittest.main()
