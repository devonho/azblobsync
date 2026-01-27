import unittest
from unittest.mock import Mock, patch, MagicMock
from src.blobhelper import (
    get_blob_service_client,
    get_container_client,
    create_folder_structure,
    create_folder_from_path,
    create_folders_from_list,
)


class TestGetBlobServiceClient(unittest.TestCase):
    """Test cases for get_blob_service_client function."""
    
    @patch('src.blobhelper.BlobServiceClient')
    @patch('src.blobhelper.DefaultAzureCredential')
    def test_get_blob_service_client_with_default_credential(self, mock_credential, mock_client):
        """Test creating a BlobServiceClient with default credentials."""
        account_url = "https://test.blob.core.windows.net"
        
        get_blob_service_client(account_url)
        
        mock_credential.assert_called_once()
        mock_client.assert_called_once_with(account_url=account_url, credential=mock_credential.return_value)
    
    @patch('src.blobhelper.BlobServiceClient')
    def test_get_blob_service_client_with_custom_credential(self, mock_client):
        """Test creating a BlobServiceClient with a custom credential."""
        account_url = "https://test.blob.core.windows.net"
        custom_credential = Mock()
        
        get_blob_service_client(account_url, custom_credential)
        
        mock_client.assert_called_once_with(account_url=account_url, credential=custom_credential)


class TestGetContainerClient(unittest.TestCase):
    """Test cases for get_container_client function."""
    
    @patch('src.blobhelper.get_blob_service_client')
    def test_get_container_client(self, mock_get_service_client):
        """Test getting a container client."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        mock_service_client = Mock()
        mock_get_service_client.return_value = mock_service_client
        
        get_container_client(account_url, container_name)
        
        mock_get_service_client.assert_called_once_with(account_url, None)
        mock_service_client.get_container_client.assert_called_once_with(container="test-container")
    
    @patch('src.blobhelper.get_blob_service_client')
    def test_get_container_client_with_credential(self, mock_get_service_client):
        """Test getting a container client with custom credential."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        custom_credential = Mock()
        mock_service_client = Mock()
        mock_get_service_client.return_value = mock_service_client
        
        get_container_client(account_url, container_name, custom_credential)
        
        mock_get_service_client.assert_called_once_with(account_url, custom_credential)


class TestCreateFolderStructure(unittest.TestCase):
    """Test cases for create_folder_structure function."""
    
    @patch('src.blobhelper.get_container_client')
    def test_create_folder_structure(self, mock_get_container_client):
        """Test creating a folder structure."""
        mock_container_client = Mock()
        mock_get_container_client.return_value = mock_container_client
        
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        folder_paths = ["folder1", "folder1/subfolder"]
        
        create_folder_structure(account_url, container_name, folder_paths)
        
        mock_get_container_client.assert_called_once_with(account_url, container_name, None)
        self.assertEqual(mock_container_client.upload_blob.call_count, 2)
    
    @patch('src.blobhelper.get_container_client')
    def test_create_folder_structure_with_trailing_slash(self, mock_get_container_client):
        """Test that folder paths with trailing slashes are handled correctly."""
        mock_container_client = Mock()
        mock_get_container_client.return_value = mock_container_client
        
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        folder_paths = ["folder1/"]  # Note trailing slash
        
        create_folder_structure(account_url, container_name, folder_paths)
        
        # Should create blob with name "folder1/.placeholder"
        mock_container_client.upload_blob.assert_called_once()
        call_args = mock_container_client.upload_blob.call_args
        self.assertEqual(call_args.kwargs['name'], "folder1/.placeholder")


class TestCreateFolderFromPath(unittest.TestCase):
    """Test cases for create_folder_from_path function."""
    
    @patch('src.blobhelper.create_folder_structure')
    def test_create_folder_from_path_single_level(self, mock_create_structure):
        """Test creating a single level folder."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        path = "folder1"
        
        create_folder_from_path(account_url, container_name, path)
        
        mock_create_structure.assert_called_once()
        call_args = mock_create_structure.call_args
        self.assertEqual(call_args[0][2], ["folder1"])  # folder_paths argument
    
    @patch('src.blobhelper.create_folder_structure')
    def test_create_folder_from_path_multiple_levels(self, mock_create_structure):
        """Test creating a nested folder structure."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        path = "level1/level2/level3"
        
        create_folder_from_path(account_url, container_name, path)
        
        mock_create_structure.assert_called_once()
        call_args = mock_create_structure.call_args
        expected_paths = ["level1", "level1/level2", "level1/level2/level3"]
        self.assertEqual(call_args[0][2], expected_paths)


class TestCreateFoldersFromList(unittest.TestCase):
    """Test cases for create_folders_from_list function."""
    
    @patch('src.blobhelper.create_folder_structure')
    def test_create_folders_from_list(self, mock_create_structure):
        """Test creating folders from a list of dictionaries."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        folder_list = [
            {'path': 'folder1', 'level': 0},
            {'path': 'folder1/subfolder1', 'level': 1},
            {'path': 'folder2', 'level': 0},
        ]
        
        create_folders_from_list(account_url, container_name, folder_list)
        
        mock_create_structure.assert_called_once()
        call_args = mock_create_structure.call_args
        expected_paths = ['folder1', 'folder1/subfolder1', 'folder2']
        self.assertEqual(call_args[0][2], expected_paths)
    
    @patch('src.blobhelper.create_folder_structure')
    def test_create_folders_from_list_empty_list(self, mock_create_structure):
        """Test creating folders from an empty list."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        folder_list = []
        
        create_folders_from_list(account_url, container_name, folder_list)
        
        mock_create_structure.assert_called_once()
        call_args = mock_create_structure.call_args
        self.assertEqual(call_args[0][2], [])
    
    @patch('src.blobhelper.create_folder_structure')
    def test_create_folders_from_list_missing_path_key(self, mock_create_structure):
        """Test that items without 'path' key are skipped."""
        account_url = "https://test.blob.core.windows.net"
        container_name = "test-container"
        folder_list = [
            {'path': 'folder1', 'level': 0},
            {'level': 1},  # Missing 'path' key
            {'path': 'folder2', 'level': 0},
        ]
        
        create_folders_from_list(account_url, container_name, folder_list)
        
        mock_create_structure.assert_called_once()
        call_args = mock_create_structure.call_args
        expected_paths = ['folder1', 'folder2']
        self.assertEqual(call_args[0][2], expected_paths)


if __name__ == '__main__':
    unittest.main()
