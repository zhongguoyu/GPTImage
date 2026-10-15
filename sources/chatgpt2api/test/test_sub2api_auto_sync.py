from __future__ import annotations

import tempfile
import threading
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

from services.sub2api_auto_sync_service import Sub2APIAutoSyncService
from services.sub2api_service import Sub2APIConfig, Sub2APISyncBusyError


class FakeImportService:
    def __init__(self) -> None:
        self.acquire_operation = mock.Mock(return_value=nullcontext())
        self.start_import = mock.Mock(return_value={"job_id": "job-1"})
        self.wait_for_job = mock.Mock(return_value={
            "status": "completed",
            "failed": 0,
            "job_id": "job-1",
            "added": 1,
            "skipped": 1,
            "refreshed": 1,
        })


class Sub2APIAutoSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.config = Sub2APIConfig(Path(self.temp_dir.name) / "sub2api.json")
        self.server = self.config.add_server(
            name="test",
            base_url="https://sub2api.example.test",
            email="",
            password="",
            api_key="test-api-key",
            group_id="",
            auto_sync_interval_minutes=5,
        )
        self.imports = FakeImportService()
        self.list_accounts = mock.Mock(return_value=[])
        self.service = Sub2APIAutoSyncService(
            self.config,
            self.imports,
            list_accounts=self.list_accounts,
            max_workers=2,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_disabled_connection_does_not_list_or_import(self):
        self.config.update_server(self.server["id"], {"auto_sync_interval_minutes": 0})

        self.assertFalse(self.service.run_once(self.server["id"], now=1700000000.0))

        self.list_accounts.assert_not_called()
        self.imports.start_import.assert_not_called()

    def test_run_once_imports_every_remote_id_and_never_deletes_local_accounts(self):
        self.list_accounts.return_value = [{"id": "a"}, {"id": "b"}, {"id": "a"}]

        self.assertTrue(self.service.run_once(self.server["id"], now=1700000000.0))

        self.imports.start_import.assert_called_once_with(
            mock.ANY,
            ["a", "b"],
            operation_lock_held=True,
        )
        state = self.config.get_server(self.server["id"])["auto_sync"]
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["last_remote_count"], 3)
        self.assertEqual(state["last_import_job_id"], "job-1")
        self.assertFalse(hasattr(self.imports, "delete_accounts"))

    def test_empty_remote_list_is_success_without_import_job(self):
        self.list_accounts.return_value = []

        self.assertTrue(self.service.run_once(self.server["id"], now=1700000000.0))

        self.imports.start_import.assert_not_called()
        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["status"],
            "completed",
        )

    def test_busy_connection_skips_cycle_and_preserves_previous_error(self):
        self.config.set_auto_sync_state(
            self.server["id"],
            status="failed",
            last_error="previous failure",
        )
        self.imports.acquire_operation.side_effect = Sub2APISyncBusyError("busy")

        self.assertFalse(self.service.run_once(self.server["id"], now=1700000000.0))

        self.list_accounts.assert_not_called()
        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["last_error"],
            "previous failure",
        )

    def test_failed_list_keeps_error_and_schedules_next_cycle(self):
        self.list_accounts.side_effect = RuntimeError("remote unavailable")

        self.assertFalse(self.service.run_once(self.server["id"], now=1700000000.0))

        state = self.config.get_server(self.server["id"])["auto_sync"]
        self.assertEqual(state["status"], "failed")
        self.assertIn("remote unavailable", state["last_error"])
        self.assertIsNotNone(state["next_run_at"])

    def test_partial_import_is_automatic_failure(self):
        self.list_accounts.return_value = [{"id": "a"}]
        self.imports.wait_for_job.return_value = {
            "status": "completed",
            "failed": 1,
            "job_id": "job-1",
        }

        self.assertFalse(self.service.run_once(self.server["id"], now=1700000000.0))

        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["status"],
            "failed",
        )

    def test_reconfigure_positive_interval_sets_immediate_deadline_and_zero_disables(self):
        self.config.update_server(self.server["id"], {"auto_sync_interval_minutes": 0})
        self.config.update_server(self.server["id"], {"auto_sync_interval_minutes": 5})

        self.service.notify_server_changed(
            self.server["id"], immediate=True, now=1700000000.0,
        )
        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["next_run_at"],
            "2023-11-14T22:13:20+00:00",
        )

        self.config.update_server(self.server["id"], {"auto_sync_interval_minutes": 0})
        self.service.notify_server_changed(
            self.server["id"], immediate=False, now=1700000001.0,
        )
        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["status"],
            "disabled",
        )

    def test_start_recovers_persisted_running_state(self):
        self.config.set_auto_sync_state(
            self.server["id"],
            status="running",
            last_error="old state",
        )
        stop_event = threading.Event()
        stop_event.set()

        self.service.start(stop_event)
        self.service.stop()

        state = self.config.get_server(self.server["id"])["auto_sync"]
        self.assertEqual(state["status"], "failed")
        self.assertIn("interrupted", state["last_error"])
        self.assertIsNotNone(state["next_run_at"])

    def test_stop_sets_stop_event_and_terminates_coordinator(self):
        self.config.set_auto_sync_state(
            self.server["id"],
            next_run_at="2999-01-01T00:00:00+00:00",
        )
        stop_event = threading.Event()

        self.service.start(stop_event)
        coordinator = self.service._coordinator_thread
        self.assertIsNotNone(coordinator)
        self.service.stop(timeout=1)

        self.assertTrue(stop_event.is_set())
        self.assertFalse(coordinator.is_alive())

    def test_running_sync_preserves_new_immediate_deadline(self):
        listed = threading.Event()
        release = threading.Event()

        def list_blocked(_server):
            listed.set()
            release.wait(timeout=2)
            return []

        self.service._list_accounts = list_blocked
        result: list[bool] = []
        worker = threading.Thread(
            target=lambda: result.append(self.service.run_once(self.server["id"], now=1700000000.0)),
        )
        worker.start()
        self.assertTrue(listed.wait(timeout=1))
        self.service.notify_server_changed(
            self.server["id"], immediate=True, now=1700000100.0,
        )
        release.set()
        worker.join(timeout=2)

        self.assertEqual(result, [True])
        self.assertEqual(
            self.config.get_server(self.server["id"])["auto_sync"]["next_run_at"],
            "2023-11-14T22:15:00+00:00",
        )


if __name__ == "__main__":
    unittest.main()
