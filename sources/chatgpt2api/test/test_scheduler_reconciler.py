from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

from services.account_service import AccountService
from services.config import ConfigStore
from services.scheduler.account_pool import AccountPool
from services.scheduler.models import AccountRequirement, Lane
from services.scheduler.reconciler import AccountReconciler
from services.storage.json_storage import JSONStorageBackend


def make_settings():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "config.json"
        path.write_text('{"auth-key":"test-auth"}', encoding="utf-8")
        settings = ConfigStore(path).get_scheduler_settings()
    lanes = dict(settings.lanes)
    lanes[Lane.IMAGE_GENERATION] = replace(
        lanes[Lane.IMAGE_GENERATION],
        per_account_concurrency=1,
    )
    return replace(settings, lanes=lanes, image_total_per_account_concurrency=1)


class SchedulerReconcilerTests(unittest.TestCase):
    def test_account_changes_advance_generation_and_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            before = service.scheduler_state_generation()

            service.add_account_items([
                {
                    "access_token": "token-1",
                    "status": "正常",
                    "type": "Plus",
                    "source_type": "web",
                    "quota": 3,
                }
            ])

            self.assertGreater(service.scheduler_state_generation(), before)
            snapshot = service.scheduler_snapshot()
            self.assertEqual(snapshot[0]["access_token"], "token-1")
            self.assertEqual(snapshot[0]["quota"], 3)

    def test_reconciler_only_snapshots_after_generation_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            pool = Mock()
            reconciler = AccountReconciler(service, pool)
            snapshot = Mock(wraps=service.scheduler_snapshot)
            service.scheduler_snapshot = snapshot

            self.assertTrue(reconciler.reconcile_if_changed())
            self.assertFalse(reconciler.reconcile_if_changed())
            self.assertEqual(snapshot.call_count, 1)
            self.assertEqual(pool.reconcile.call_count, 1)

            service.update_account("token-1", {"quota": 0, "status": "限流"})

            self.assertTrue(reconciler.reconcile_if_changed())
            self.assertEqual(snapshot.call_count, 2)
            self.assertEqual(pool.reconcile.call_count, 2)

    def test_status_change_removes_account_from_ready_heap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([
                {
                    "access_token": "token-1",
                    "status": "正常",
                    "type": "Plus",
                    "source_type": "web",
                    "quota": 3,
                }
            ])
            pool = AccountPool(service.scheduler_snapshot(), make_settings())
            reconciler = AccountReconciler(service, pool)
            reconciler.reconcile_if_changed()
            requirement = AccountRequirement(
                Lane.IMAGE_GENERATION,
                "gpt-image-2",
                source_type="web",
            )

            self.assertIsNotNone(pool.try_acquire(requirement, deadline=pool.clock() + 1))
            service.update_account("token-1", {"quota": 0, "status": "限流"})
            reconciler.reconcile_if_changed()

            self.assertIsNone(pool.try_acquire(requirement, deadline=pool.clock() + 1))

    def test_start_and_stop_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            pool = Mock()
            reconciler = AccountReconciler(service, pool, interval_seconds=1)

            reconciler.start()
            reconciler.start()
            reconciler.stop(timeout=1)
            reconciler.stop(timeout=1)

            self.assertIsNone(reconciler._thread)
            self.assertEqual(pool.reconcile.call_count, 1)


if __name__ == "__main__":
    unittest.main()
