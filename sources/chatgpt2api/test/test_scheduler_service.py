from __future__ import annotations

import tempfile
import unittest
import sys
from types import ModuleType
from dataclasses import replace
from pathlib import Path
from unittest import mock

from services.config import ConfigStore
from services.scheduler.models import AccountRequirement, Lane
from services.scheduler.service import SchedulerService
from services.scheduler.state_store import SchedulerStateStore


class FakeAccountService:
    def __init__(self) -> None:
        self.accounts = [
            {
                "access_token": "secret-token",
                "type": "Plus",
                "status": "正常",
                "source_type": "web",
                "quota": 5,
            }
        ]
        self.generation = 1

    def scheduler_snapshot(self):
        return [dict(item) for item in self.accounts]

    def scheduler_state_generation(self):
        return self.generation


class FakeClosable:
    def __init__(self) -> None:
        self.closed_current_thread = 0

    def close_current_thread(self) -> None:
        self.closed_current_thread += 1


class FakeJob:
    def __init__(self, lane: Lane) -> None:
        self.requirement = AccountRequirement(lane, "auto")


def make_settings():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "config.json"
        path.write_text('{"auth-key":"test-auth"}', encoding="utf-8")
        settings = ConfigStore(path).get_scheduler_settings()
    lanes = dict(settings.lanes)
    for lane in Lane:
        lanes[lane] = replace(
            lanes[lane],
            worker_count=1,
            queue_wait_timeout_ms=100,
        )
    return replace(settings, lanes=lanes, account_reconcile_interval_ms=100)


class SchedulerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_and_stop_manage_all_lanes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = SchedulerService(
                FakeAccountService(),
                settings=make_settings(),
                state_store=SchedulerStateStore(Path(tmp_dir) / "scheduler.db"),
                model_router=lambda _model: {"plus"},
            )

            await service.start()
            self.assertTrue(service.lane(Lane.TEXT).running)
            self.assertTrue(service.lane(Lane.IMAGE_GENERATION).running)
            self.assertTrue(service.lane(Lane.IMAGE_EDIT).running)
            await service.stop()
            await service.stop()
            self.assertFalse(service.lane(Lane.TEXT).running)

    def test_classifier_assigns_image_edit_lane_and_cost(self) -> None:
        service = SchedulerService(
            FakeAccountService(),
            settings=make_settings(),
            state_store=SchedulerStateStore(Path(tempfile.mkdtemp()) / "scheduler.db"),
            model_router=lambda _model: {"plus"},
        )

        request = service.classify_image_edit({"model": "gpt-image-2", "n": 4})

        self.assertEqual(request.lane, Lane.IMAGE_EDIT)
        self.assertEqual(request.cost, 4)

    def test_classifier_does_not_restrict_regular_images_to_web_accounts(self) -> None:
        service = SchedulerService(
            FakeAccountService(),
            settings=make_settings(),
            state_store=SchedulerStateStore(Path(tempfile.mkdtemp()) / "scheduler.db"),
            model_router=lambda _model: {"plus"},
        )

        regular = service.classify_image_generation({"model": "gpt-image-2", "n": 1})
        codex = service.classify_image_generation({"model": "plus-codex-gpt-image-2", "n": 1})

        self.assertIsNone(regular.source_type)
        self.assertEqual(codex.source_type, "codex")
        self.assertEqual(codex.plan_type, "plus")

    def test_metrics_redact_account_token(self) -> None:
        service = SchedulerService(
            FakeAccountService(),
            settings=make_settings(),
            state_store=SchedulerStateStore(Path(tempfile.mkdtemp()) / "scheduler.db"),
            model_router=lambda _model: {"plus"},
        )

        metrics = service.metrics_snapshot()

        serialized = str(metrics)
        self.assertNotIn("secret-token", serialized)
        self.assertIn("account_ref", serialized)

    def test_worker_cleanup_closes_thread_resources_and_trims_only_image_jobs(self) -> None:
        account_service = FakeAccountService()
        account_service.storage = FakeClosable()
        state_store = FakeClosable()
        quota_module = ModuleType("services.quota_service")
        quota_module.quota_service = FakeClosable()
        service = SchedulerService(
            account_service,
            settings=make_settings(),
            state_store=state_store,
            model_router=lambda _model: {"plus"},
        )

        with (
            mock.patch.dict(sys.modules, {"services.quota_service": quota_module}),
            mock.patch("services.scheduler.service._malloc_trim") as malloc_trim,
        ):
            service._cleanup_worker_thread(FakeJob(Lane.TEXT))
            self.assertEqual(state_store.closed_current_thread, 1)
            self.assertEqual(account_service.storage.closed_current_thread, 1)
            self.assertEqual(quota_module.quota_service.closed_current_thread, 1)
            malloc_trim.assert_not_called()

            service._cleanup_worker_thread(FakeJob(Lane.IMAGE_GENERATION))
            self.assertEqual(state_store.closed_current_thread, 2)
            self.assertEqual(account_service.storage.closed_current_thread, 2)
            self.assertEqual(quota_module.quota_service.closed_current_thread, 2)
            malloc_trim.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
