from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from pathlib import Path

from services.log_store import SQLiteLogStore


class SQLiteLogStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "logs.db"
        self.legacy_path = self.root / "logs.jsonl"
        self.store = SQLiteLogStore(self.db_path, self.legacy_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _raw_value(self, sql: str, parameter: object = None):
        connection = sqlite3.connect(self.db_path)
        try:
            if parameter is None:
                row = connection.execute(sql).fetchone()
            else:
                row = connection.execute(sql, (parameter,)).fetchone()
            return None if row is None else row[0]
        finally:
            connection.close()

    def test_initialize_creates_schema_and_pragmas(self):
        self.store.initialize()

        table_name = self._raw_value(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'logs'"
        )
        self.assertEqual(table_name, "logs")
        self.assertEqual(str(self._raw_value("PRAGMA journal_mode")).lower(), "wal")
        foreign_keys = self.store._connection().execute("PRAGMA foreign_keys").fetchone()[0]
        self.assertEqual(foreign_keys, 1)

    def test_close_releases_connection(self):
        self.store.initialize()
        self.store.add("info", "ok", {})
        connection = self.store._connection()

        self.store.close()

        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_add_round_trips_public_shape_and_detail(self):
        self.store.initialize()

        item = self.store.add("call", "调用完成", {"status": 200})

        self.assertTrue(item["id"])
        self.assertRegex(item["time"], r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(item["type"], "call")
        self.assertEqual(item["summary"], "调用完成")
        self.assertEqual(item["detail"], {"status": 200})

    def test_detail_is_sanitized_recursively_and_bounded(self):
        self.store.initialize()

        item = self.store.add(
            "call",
            "bounded",
            {
                "status": 200,
                "nested": {
                    "response": "sensitive response",
                    "safe": "x" * 10000,
                },
            },
        )

        self.assertNotIn("response", item["detail"]["nested"])
        self.assertEqual(len(item["detail"]["nested"]["safe"]), 4096)
        stored_size = self._raw_value("SELECT length(detail_json) FROM logs WHERE id = ?", item["id"])
        self.assertLessEqual(stored_size, 64 * 1024)

    def test_legacy_migration_drops_nested_payload_fields(self):
        self.legacy_path.write_text(
            json.dumps({
                "type": "call",
                "summary": "legacy",
                "detail": {
                    "request": {"body": "x" * 100000},
                    "safe": {"status": 200},
                },
            }) + "\n",
            encoding="utf-8",
        )

        self.store.initialize()

        item = self.store.query(page=1, page_size=100)["items"][0]
        self.assertNotIn("request", item["detail"])
        self.assertEqual(item["detail"]["safe"], {"status": 200})

    def test_query_returns_newest_first_and_has_total_and_has_more(self):
        self.store.initialize()
        for index in range(3):
            self.store.add("call", f"call-{index}", {"index": index})

        result = self.store.query(page=1, page_size=2)

        self.assertEqual(result["total"], 3)
        self.assertTrue(result["has_more"])
        self.assertEqual(
            [item["summary"] for item in result["items"]],
            ["call-2", "call-1"],
        )

    def test_type_and_date_filters_are_applied_by_sql(self):
        self.store.initialize()
        self.store.add("call", "call", {})
        self.store.add("account", "account", {})

        result = self.store.query(
            type_filter="account",
            start_date="2099-01-01",
            end_date="2099-01-31",
        )

        self.assertEqual(result["total"], 0)

    def test_delete_by_ids_is_transactional_and_idempotent(self):
        self.store.initialize()
        item = self.store.add("call", "delete me", {})

        self.assertEqual(self.store.delete([item["id"], "missing-id"]), 1)
        self.assertEqual(self.store.delete([item["id"]]), 0)

    def test_migrates_legacy_jsonl_once_and_keeps_metadata(self):
        self.legacy_path.write_text(
            json.dumps(
                {
                    "id": "old-1",
                    "time": "2026-09-01 10:00:00",
                    "type": "call",
                    "summary": "old",
                    "detail": {"status": 200},
                }
            )
            + "\n"
            + json.dumps(
                {
                    "time": "2026-09-01 10:01:00",
                    "type": "account",
                    "summary": "legacy",
                    "detail": {},
                }
            )
            + "\nnot-json\n[]\n",
            encoding="utf-8",
        )

        self.store.initialize()

        result = self.store.query(page=1, page_size=100)
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["items"][1]["id"], "old-1")
        self.assertTrue(result["items"][0]["id"])
        self.assertFalse(self.legacy_path.exists())
        self.assertTrue((self.root / "logs.jsonl.migrated").exists())
        metadata = self.store.migration_metadata()
        self.assertEqual(metadata["imported"], 2)
        self.assertEqual(metadata["invalid_lines"], 2)

        self.store.initialize()
        self.assertEqual(self.store.query(page=1, page_size=100)["total"], 2)

    def test_migration_uses_timestamped_backup_if_default_exists(self):
        self.legacy_path.write_text("{}\n", encoding="utf-8")
        (self.root / "logs.jsonl.migrated").write_text("existing\n", encoding="utf-8")

        self.store.initialize()

        backups = list(self.root.glob("logs.jsonl.migrated.*"))
        self.assertEqual(len(backups), 1)
        self.assertFalse(self.legacy_path.exists())

    def test_migration_failure_keeps_source_file(self):
        self.legacy_path.write_text(
            json.dumps({"type": "call", "summary": "will fail", "detail": {}}) + "\n",
            encoding="utf-8",
        )

        with mock.patch.object(self.store, "_insert_legacy_batch", side_effect=RuntimeError("db failure"), create=True):
            with self.assertRaises(RuntimeError):
                self.store.initialize()

        self.assertTrue(self.legacy_path.exists())

    def test_cleanup_expired_removes_only_old_rows(self):
        self.store.initialize()
        now = time.time()
        old_item = self.store.add("call", "old", {}, created_ts=now - 3 * 86400)
        new_item = self.store.add("call", "new", {}, created_ts=now)

        self.assertEqual(self.store.cleanup_expired(2), 1)
        remaining = self.store.query(page=1, page_size=100)
        self.assertEqual([item["id"] for item in remaining["items"]], [new_item["id"]])
        self.assertNotEqual(old_item["id"], new_item["id"])

    def test_concurrent_writes_from_multiple_threads_are_readable(self):
        self.store.initialize()

        def write(index: int):
            return self.store.add("call", f"call-{index}", {"index": index})

        with ThreadPoolExecutor(max_workers=8) as executor:
            items = list(executor.map(write, range(40)))

        result = self.store.query(page=1, page_size=100)
        self.assertEqual(result["total"], 40)
        self.assertEqual(len({item["id"] for item in items}), 40)


if __name__ == "__main__":
    unittest.main()
