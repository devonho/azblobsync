import os
import sys
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import main as app_main


class TestMainModeSelection(unittest.TestCase):
    def test_selects_target_purge_mode_when_only_target_env_is_set(self):
        env = {
            "TARGET_AZURE_STORAGE_ACCOUNT_URL": "https://target.blob.core.windows.net",
            "TARGET_AZURE_STORAGE_CONTAINER_NAME": "target-container",
            "LOOP_INTERVAL_MINUTES": "0",
        }

        with patch.dict(os.environ, env, clear=True):
            with patch.object(app_main, "purge_target_blob_container_target_main") as purge_mock, \
                 patch.object(app_main, "blob_container_source_blob_container_target_main") as container_mock, \
                 patch.object(app_main, "local_source_blob_container_target") as local_mock:
                app_main.main()

                purge_mock.assert_called_once()
                container_mock.assert_not_called()
                local_mock.assert_not_called()


class TestPurgeMode(unittest.TestCase):
    def test_purge_mode_skips_delete_when_skip_delete_true(self):
        env = {
            "TARGET_AZURE_STORAGE_ACCOUNT_URL": "https://target.blob.core.windows.net",
            "TARGET_AZURE_STORAGE_CONTAINER_NAME": "target-container",
            "SKIP_DELETE": "true",
        }

        fake_blob_1 = SimpleNamespace(name="a.txt")
        fake_blob_2 = SimpleNamespace(name="b/c.txt")

        with patch.dict(os.environ, env, clear=True):
            with patch.object(app_main, "get_container_client") as get_client_mock:
                client = get_client_mock.return_value
                client.list_blobs.return_value = [fake_blob_1, fake_blob_2]

                result = app_main.purge_target_blob_container_target_main()

                client.delete_blob.assert_not_called()
                self.assertEqual(result["deleted"], [])
                self.assertEqual(result["skipped_by_skip_delete"], ["a.txt", "b/c.txt"])


if __name__ == "__main__":
    unittest.main()
