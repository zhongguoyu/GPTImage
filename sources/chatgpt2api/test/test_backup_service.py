from __future__ import annotations

import io
import sqlite3
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.backup_service as backup_module
from services.backup_service import BackupService


class BackupQuotaSnapshotTests(unittest.TestCase):
    def test_auth_key_snapshot_includes_consistent_quota_database_snapshot(self):
        quota_bytes = b"SQLite quota snapshot"
        backend = mock.Mock()
        backend.get_backend_info.return_value = {"backend": "test"}
        backend.load_auth_keys.return_value = [{"id": "key-1"}]
        quota = mock.Mock()
        quota.snapshot_bytes.return_value = quota_bytes
        service = BackupService(quota_service_instance=quota)

        with mock.patch.object(backup_module, "config") as config:
            config.app_version = "test"
            config.get_storage_backend.return_value = backend
            payload = service._build_backup_archive(
                {"include": {"auth_keys_snapshot": True}},
                trigger="manual",
            )

        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            self.assertEqual(archive.extractfile("data/auth_quota.db").read(), quota_bytes)
        quota.snapshot_bytes.assert_called_once_with()

    def test_archive_writer_uses_seekable_temporary_file(self):
        service = BackupService()

        with mock.patch.object(backup_module, "config") as config:
            config.app_version = "test"
            config.get_storage_backend.return_value.get_backend_info.return_value = {"backend": "test"}
            with service._build_backup_archive_file(
                {"include": {}},
                trigger="manual",
            ) as archive_file:
                self.assertTrue(archive_file.seekable())
                self.assertFalse(isinstance(archive_file, io.BytesIO))
                archive_file.seek(0)
                with tarfile.open(fileobj=archive_file, mode="r:gz") as archive:
                    self.assertIn("backup-metadata.json", archive.getnames())

    def test_archive_contains_consistent_sqlite_state_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "app.db"
            connection = sqlite3.connect(state_path)
            try:
                connection.execute("CREATE TABLE state (value TEXT)")
                connection.execute("INSERT INTO state VALUES ('current')")
                connection.commit()
            finally:
                connection.close()

            service = BackupService()
            with (
                mock.patch.object(backup_module, "DATA_DIR", root),
                mock.patch.object(backup_module, "config") as config,
            ):
                config.app_version = "test"
                config.app_db_path = state_path
                config.get_storage_backend.return_value.get_backend_info.return_value = {"backend": "test"}
                with service._build_backup_archive_file(
                    {"include": {"state_db": True}},
                    trigger="manual",
                ) as archive_file:
                    with tarfile.open(fileobj=archive_file, mode="r:gz") as archive:
                        with tempfile.NamedTemporaryFile(suffix=".db") as extracted:
                            extracted.write(archive.extractfile("data/app.db").read())
                            extracted.flush()
                            snapshot = sqlite3.connect(extracted.name)
                            try:
                                value = snapshot.execute("SELECT value FROM state").fetchone()[0]
                            finally:
                                snapshot.close()

            self.assertEqual(value, "current")


if __name__ == "__main__":
    unittest.main()
