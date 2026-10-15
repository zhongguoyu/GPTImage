from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("CHATGPT2API_AUTH_KEY", "test-auth")

from services.config import ConfigStore
from services.scheduler.models import Lane


class SchedulerConfigTests(unittest.TestCase):
    def make_store(self, data: dict[str, object]) -> ConfigStore:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        path = Path(self.tmp_dir.name) / "config.json"
        path.write_text(json.dumps({"auth-key": "test-auth", **data}), encoding="utf-8")
        return ConfigStore(path)

    def test_scheduler_defaults_are_bounded_and_exposed(self) -> None:
        settings = self.make_store({}).get_scheduler_settings()

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.system_concurrency, 192)
        self.assertEqual(settings.lanes[Lane.TEXT].worker_count, 48)
        self.assertEqual(settings.lanes[Lane.TEXT].global_concurrency, 128)
        self.assertEqual(settings.lanes[Lane.IMAGE_GENERATION].worker_count, 24)
        self.assertEqual(settings.lanes[Lane.IMAGE_GENERATION].global_concurrency, 32)
        self.assertEqual(settings.lanes[Lane.IMAGE_EDIT].worker_count, 24)
        self.assertEqual(settings.lanes[Lane.IMAGE_EDIT].global_concurrency, 32)
        self.assertGreater(settings.lanes[Lane.TEXT].request_timeout_ms, 0)
        self.assertGreater(settings.lanes[Lane.IMAGE_GENERATION].queue_max_cost, 0)
        self.assertGreater(settings.image_total_per_account_concurrency, 0)

    def test_image_input_default_budget_is_bounded_for_small_hosts(self) -> None:
        store = self.make_store({})

        self.assertEqual(store.image_input_max_bytes, 32 * 1024 * 1024)
        self.assertEqual(store.get()["image_input_max_bytes"], 32 * 1024 * 1024)

    def test_invalid_scheduler_values_use_safe_bounds(self) -> None:
        settings = self.make_store({
            "scheduler": {
                "system_concurrency": 0,
                "text": {"worker_count": -3, "queue_max_items": "bad"},
            },
        }).get_scheduler_settings()

        self.assertEqual(settings.system_concurrency, 1)
        self.assertEqual(settings.lanes[Lane.TEXT].worker_count, 1)
        self.assertGreaterEqual(settings.lanes[Lane.TEXT].queue_max_items, 1)

    def test_lane_capacity_cannot_exceed_system_capacity(self) -> None:
        settings = self.make_store({
            "scheduler": {
                "system_concurrency": 4,
                "text": {"global_concurrency": 100},
            },
        }).get_scheduler_settings()

        self.assertLessEqual(settings.lanes[Lane.TEXT].global_concurrency, 4)

    def test_legacy_image_account_concurrency_is_used_as_total_limit(self) -> None:
        settings = self.make_store({"image_account_concurrency": 7}).get_scheduler_settings()

        self.assertEqual(settings.image_total_per_account_concurrency, 7)


if __name__ == "__main__":
    unittest.main()
