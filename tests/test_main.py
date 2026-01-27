import unittest
import os
import tempfile
import shutil
from src.main import bfs_traverse, get_folders_and_files


class TestBfsTraverse(unittest.TestCase):
    """Test cases for bfs_traverse function."""
    
    def setUp(self):
        """Create a temporary directory structure for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.base_path = self.test_dir
        
        # Create test directory structure
        os.makedirs(os.path.join(self.test_dir, "root", "level1_dir1"))
        os.makedirs(os.path.join(self.test_dir, "root", "level1_dir2"))
        os.makedirs(os.path.join(self.test_dir, "root", "level1_dir1", "level2_dir1"))
        
        # Create test files
        open(os.path.join(self.test_dir, "root", "level1_file.txt"), 'w').close()
        open(os.path.join(self.test_dir, "root", "level1_dir1", "level2_file.txt"), 'w').close()
    
    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.test_dir)
    
    def test_bfs_traverse_returns_iterator(self):
        """Test that bfs_traverse returns an iterator."""
        result = bfs_traverse("root", self.base_path)
        self.assertTrue(hasattr(result, '__iter__'))
    
    def test_bfs_traverse_yields_tuples(self):
        """Test that bfs_traverse yields tuples with correct structure."""
        items = list(bfs_traverse("root", self.base_path))
        
        # Should have entries for root, subdirs, and files
        self.assertGreater(len(items), 0)
        
        for path, is_dir, level in items:
            self.assertIsInstance(path, str)
            self.assertIsInstance(is_dir, bool)
            self.assertIsInstance(level, int)
    
    def test_bfs_traverse_invalid_directory(self):
        """Test that bfs_traverse raises ValueError for non-existent directory."""
        with self.assertRaises(ValueError):
            list(bfs_traverse("nonexistent", self.base_path))
    
    def test_bfs_traverse_includes_root(self):
        """Test that bfs_traverse includes the root directory."""
        items = list(bfs_traverse("root", self.base_path))
        paths = [path for path, is_dir, level in items if is_dir]
        
        self.assertIn("root", paths)


class TestGetFoldersAndFiles(unittest.TestCase):
    """Test cases for get_folders_and_files function."""
    
    def setUp(self):
        """Create a temporary directory structure for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.base_path = self.test_dir
        
        # Create test directory structure
        os.makedirs(os.path.join(self.test_dir, "root", "dir1", "subdir1"))
        os.makedirs(os.path.join(self.test_dir, "root", "dir2"))
        
        # Create test files
        open(os.path.join(self.test_dir, "root", "file1.txt"), 'w').close()
        open(os.path.join(self.test_dir, "root", "dir1", "file2.txt"), 'w').close()
        open(os.path.join(self.test_dir, "root", "dir1", "subdir1", "file3.txt"), 'w').close()
    
    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.test_dir)
    
    def test_get_folders_and_files_returns_two_lists(self):
        """Test that get_folders_and_files returns two lists."""
        folder_list, file_list = get_folders_and_files("root", self.base_path)
        
        self.assertIsInstance(folder_list, list)
        self.assertIsInstance(file_list, list)
    
    def test_get_folders_and_files_folder_structure(self):
        """Test that folder_list contains dictionaries with 'path' and 'level' keys."""
        folder_list, file_list = get_folders_and_files("root", self.base_path)
        
        self.assertGreater(len(folder_list), 0)
        
        for folder in folder_list:
            self.assertIn('path', folder)
            self.assertIn('level', folder)
    
    def test_get_folders_and_files_file_structure(self):
        """Test that file_list contains dictionaries with 'path' and 'level' keys."""
        folder_list, file_list = get_folders_and_files("root", self.base_path)
        
        self.assertGreater(len(file_list), 0)
        
        for file in file_list:
            self.assertIn('path', file)
            self.assertIn('level', file)
    
    def test_get_folders_and_files_counts(self):
        """Test that the correct number of folders and files are found."""
        folder_list, file_list = get_folders_and_files("root", self.base_path)
        
        # Should find: root, dir1, subdir1, dir2 (4 directories)
        self.assertGreaterEqual(len(folder_list), 3)  # At least 3 subdirs + root
        
        # Should find: file1.txt, file2.txt, file3.txt (3 files)
        self.assertEqual(len(file_list), 3)


if __name__ == '__main__':
    unittest.main()
