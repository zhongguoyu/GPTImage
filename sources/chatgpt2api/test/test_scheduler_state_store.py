from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from services.scheduler.state_store import SchedulerStateStore, SnapshotWriter, TaskStateWriter


class SchedulerStateStoreTests(unittest.TestCase):
    def test_health_snapshot_upsert_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.initialize()
            store.upsert_health({
                "account_key": "a",
                "lane": "text",
                "breaker_state": "open",
            })
            store.upsert_health({
                "account_key": "a",
                "lane": "text",
                "breaker_state": "half_open",
            })

            rows = store.list_health()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["breaker_state"], "half_open")

    def test_sqlite_uses_wal_and_busy_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.initialize()
            connection = store._connection()

            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone()[0], 5000)

    def test_concurrent_health_upserts_keep_one_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.initialize()

            def write(index: int) -> None:
                store.upsert_health({
                    "account_key": "a",
                    "lane": "text",
                    "breaker_state": f"state-{index}",
                })

            threads = [threading.Thread(target=write, args=(index,)) for index in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(len(store.list_health()), 1)

    def test_close_releases_connections_created_by_worker_threads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.initialize()

            connections = []

            def read() -> None:
                connections.append(store._connection())
                store.list_health()

            threads = [threading.Thread(target=read) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            store.close()

            self.assertEqual(store.connection_count(), 0)
            for connection in connections:
                with self.assertRaises(sqlite3.ProgrammingError):
                    connection.execute("SELECT 1")

    def test_snapshot_writer_coalesces_and_preserves_failed_batch(self) -> None:
        store = Mock()
        store.upsert_health_batch.side_effect = OSError("busy")
        writer = SnapshotWriter(store, pending_threshold=2)
        writer.submit({"account_key": "a", "lane": "text", "breaker_state": "open"})
        writer.submit({"account_key": "a", "lane": "text", "breaker_state": "closed"})

        with self.assertRaises(OSError):
            writer.flush()

        self.assertEqual(writer.pending_count(), 1)
        self.assertEqual(store.upsert_health_batch.call_args.args[0][0]["breaker_state"], "closed")

    def test_unknown_submission_is_not_retried_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.initialize()
            store.save_task({
                "task_key": "owner:task",
                "status": "unknown",
                "remote_task_id": None,
            })

            restored = store.load_recoverable_tasks()

            self.assertEqual(restored[0]["status"], "unknown")
            self.assertFalse(restored[0]["retryable"])

    def test_known_remote_task_is_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.save_task({
                "owner_id": "owner",
                "task_id": "task",
                "status": "running",
                "remote_task_id": "remote-1",
            })

            restored = store.load_recoverable_tasks()

            self.assertTrue(restored[0]["retryable"])

    def test_queued_task_without_remote_id_becomes_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.save_task({
                "owner_id": "owner",
                "task_id": "queued-task",
                "status": "queued",
            })

            restored = store.load_recoverable_tasks()

            self.assertEqual(restored[0]["status"], "unknown")
            self.assertEqual(store.get_task("owner:queued-task")["status"], "unknown")

    def test_tasks_are_stored_in_normalized_tasks_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "app.db")
            store.save_task({
                "task_key": "owner:task",
                "owner_id": "owner",
                "task_id": "task",
                "mode": "image_generation",
                "status": "completed",
                "updated_at": 10,
            })

            normalized_count = store._connection().execute(
                "SELECT COUNT(*) FROM tasks"
            ).fetchone()[0]
            legacy_count = store._connection().execute(
                "SELECT COUNT(*) FROM scheduler_tasks"
            ).fetchone()[0] if store._table_exists("scheduler_tasks") else 0

            self.assertEqual(normalized_count, 1)
            self.assertEqual(legacy_count, 0)

    def test_task_type_filter_keeps_image_and_file_tasks_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "app.db")
            store.save_task({
                "task_key": "owner:image",
                "owner_id": "owner",
                "task_id": "image",
                "mode": "generate",
                "status": "success",
            })
            store.save_task({
                "task_key": "owner:file",
                "owner_id": "owner",
                "task_id": "file",
                "mode": "editable_ppt",
                "status": "success",
            })

            image_tasks = store.list_tasks("owner", task_types=("generate", "edit"))
            file_tasks = store.list_tasks("owner", task_types=("editable_ppt", "editable_psd"))

            self.assertEqual([item["task_id"] for item in image_tasks], ["image"])
            self.assertEqual([item["task_id"] for item in file_tasks], ["file"])

    def test_cleanup_tasks_removes_old_terminal_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "app.db")
            store.save_task({
                "task_key": "owner:old",
                "owner_id": "owner",
                "task_id": "old",
                "status": "completed",
                "updated_at": 1,
            })

            removed = store.cleanup_tasks(now=100, retention_seconds=10, max_rows=100)

            self.assertEqual(removed, 1)
            self.assertIsNone(store.get_task("owner:old"))

    def test_snapshot_writer_wakes_on_pending_threshold(self) -> None:
        store = Mock()
        writer = SnapshotWriter(store, interval_seconds=60, pending_threshold=1)
        writer.start()
        writer.submit({"account_key": "a", "lane": "text", "breaker_state": "open"})

        deadline = time.time() + 1
        while time.time() < deadline and store.upsert_health_batch.call_count == 0:
            time.sleep(0.01)
        writer.stop(timeout=1)

        self.assertGreaterEqual(store.upsert_health_batch.call_count, 1)

    def test_task_state_writer_coalesces_by_task_key(self) -> None:
        store = Mock()
        writer = TaskStateWriter(store)
        writer.submit({"task_key": "owner:task", "status": "running"})
        writer.submit({"task_key": "owner:task", "status": "success"})

        writer.flush()

        store.save_task_batch.assert_called_once_with([
            {"task_key": "owner:task", "status": "success"},
        ])
        self.assertEqual(writer.pending_count(), 0)

    def test_legacy_import_renames_only_after_commit_and_drops_raw_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "image_tasks.json"
            source.write_text(
                json.dumps({
                    "tasks": [
                        {
                            "id": "task-1",
                            "owner_id": "owner",
                            "status": "running",
                            "mode": "generate",
                            "model": "gpt-image-2",
                            "conversation_id": "remote-1",
                            "prompt": "secret prompt",
                            "images": [{"b64_json": "secret-image"}],
                        },
                        {
                            "id": "task-2",
                            "owner_id": "owner",
                            "status": "queued",
                            "mode": "edit",
                        },
                    ]
                }),
                encoding="utf-8",
            )
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")

            result = store.import_legacy_tasks(source)

            self.assertFalse(source.exists())
            self.assertTrue(Path(result["path"]).exists())
            self.assertEqual(result["imported"], 2)
            rows = store.load_recoverable_tasks()
            by_id = {row["task_id"]: row for row in rows}
            self.assertTrue(by_id["task-1"]["retryable"])
            self.assertFalse(by_id["task-2"]["retryable"])
            self.assertNotIn("secret", json.dumps(by_id, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
