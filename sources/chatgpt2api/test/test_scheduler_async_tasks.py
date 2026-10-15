from __future__ import annotations

import asyncio
import base64
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from services.image_task_service import ImageTaskService
from services.quota_service import AuthQuotaStore, QuotaService
from services.scheduler.errors import QueueFullError
from services.scheduler.models import AccountRequirement, Lane
from services.scheduler.state_store import SchedulerStateStore


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}
USER_OWNER = {"id": "user-1", "name": "User", "role": "user"}


class FakeScheduler:
    def __init__(self, *, reject: bool = False, fail_background: bool = False) -> None:
        self.reject = reject
        self.fail_background = fail_background
        self.requirements: list[AccountRequirement] = []

    def classify_image_generation(self, payload):
        requirement = AccountRequirement(Lane.IMAGE_GENERATION, str(payload.get("model")), cost=1)
        self.requirements.append(requirement)
        return requirement

    async def admit_background(self, requirement, _work):
        if self.reject:
            raise QueueFullError("scheduler queue is full")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        if self.fail_background:
            future.set_exception(RuntimeError("background task failed"))
        else:
            future.set_result(None)
        return future


class SchedulerAsyncTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_submit_is_admitted_without_creating_a_task_thread(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            scheduler = FakeScheduler()
            service = ImageTaskService(
                Path(tmp_dir) / "tasks.json",
                generation_handler=lambda _payload: {"data": []},
                scheduler_service_instance=scheduler,
            )
            before = {thread.ident for thread in threading.enumerate()}
            result = await service.submit_generation_async(
                OWNER,
                client_task_id="queued-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
            )
            after = {thread.ident for thread in threading.enumerate()}

            self.assertEqual(result["status"], "queued")
            self.assertEqual(after - before, set())
            self.assertEqual(len(scheduler.requirements), 1)

    async def test_scheduler_rejection_removes_unadmitted_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = ImageTaskService(
                Path(tmp_dir) / "tasks.json",
                scheduler_service_instance=FakeScheduler(reject=True),
            )
            with self.assertRaises(QueueFullError):
                await service.submit_generation_async(
                    OWNER,
                    client_task_id="rejected-task",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )
            self.assertEqual(service.list_tasks(OWNER, ["rejected-task"])["items"], [])

    async def test_scheduler_rejection_releases_image_quota_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = QuotaService(AuthQuotaStore(Path(tmp_dir) / "auth_quota.db"))
            quota.set_limits("user-1", image_limit=1)
            service = ImageTaskService(
                Path(tmp_dir) / "tasks.json",
                scheduler_service_instance=FakeScheduler(reject=True),
                quota_service_instance=quota,
            )
            with self.assertRaises(QueueFullError):
                await service.submit_generation_async(
                    USER_OWNER,
                    client_task_id="quota-rejected-task",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )

            self.assertEqual(quota.get_quota("user-1")["image_reserved"], 0)
            self.assertEqual(quota.list_reservations("user-1")[0]["status"], "released")

    async def test_background_failure_updates_task_after_admission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = ImageTaskService(
                Path(tmp_dir) / "tasks.json",
                scheduler_service_instance=FakeScheduler(fail_background=True),
            )
            await service.submit_generation_async(
                OWNER,
                client_task_id="failed-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
            )
            for _ in range(100):
                task = service.list_tasks(OWNER, ["failed-task"])["items"][0]
                if task.get("status") == "error":
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(task["status"], "error")
            self.assertIn("background task failed", task["error"])

    def test_sqlite_task_metadata_excludes_image_result_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            encoded = base64.b64encode(b"generated-image").decode("ascii")
            service = ImageTaskService(
                Path(tmp_dir) / "legacy-image-tasks.json",
                generation_handler=lambda _payload: {"data": [{"b64_json": encoded}]},
                state_store=store,
            )
            service.submit_generation(
                OWNER,
                client_task_id="sqlite-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
            )
            for _ in range(100):
                task = service.list_tasks(OWNER, ["sqlite-task"])["items"][0]
                if task.get("status") == "success":
                    break
                time.sleep(0.01)
            record = store.get_task("owner-1:sqlite-task")
            self.assertIsNotNone(record)
            self.assertFalse(record["result"].get("available"))
            self.assertEqual(record["result"].get("count"), 1)
            self.assertNotIn("b64_json", str(record["metadata"]))

    def test_remote_conversation_id_is_persisted_during_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")

            def handler(payload):
                payload["progress_callback"]("image_conversation_id", "conversation-live")
                return {"data": [{"url": "https://files.example.test/generated.png"}]}

            service = ImageTaskService(
                Path(tmp_dir) / "legacy-image-tasks.json",
                generation_handler=handler,
                state_store=store,
            )
            service.submit_generation(
                OWNER,
                client_task_id="remote-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
            )
            for _ in range(100):
                task = service.list_tasks(OWNER, ["remote-task"])["items"][0]
                if task.get("status") == "success":
                    break
                time.sleep(0.01)

            record = store.get_task("owner-1:remote-task")
            self.assertEqual(record["remote_task_id"], "conversation-live")
            self.assertTrue(record["result"]["available"])

            reloaded = ImageTaskService(
                Path(tmp_dir) / "legacy-image-tasks.json",
                state_store=store,
            )
            restored = reloaded.list_tasks(OWNER, ["remote-task"])["items"][0]
            self.assertEqual(restored["status"], "success")
            self.assertEqual(restored["data"][0]["url"], "https://files.example.test/generated.png")

    async def test_restart_marks_unknown_and_resumes_known_remote_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            store.save_task({
                "owner_id": "owner-1",
                "task_id": "known-task",
                "status": "running",
                "model": "gpt-image-2",
                "remote_task_id": "conversation-1",
            })
            store.save_task({
                "owner_id": "owner-1",
                "task_id": "unknown-task",
                "status": "queued",
                "model": "gpt-image-2",
            })
            service = ImageTaskService(
                Path(tmp_dir) / "legacy-image-tasks.json",
                scheduler_service_instance=FakeScheduler(),
                state_store=store,
            )

            known = service.list_tasks(OWNER, ["known-task"])["items"][0]
            unknown = service.list_tasks(OWNER, ["unknown-task"])["items"][0]
            self.assertEqual(known["status"], "running")
            self.assertEqual(known["conversation_id"], "conversation-1")
            self.assertEqual(unknown["status"], "error")
            self.assertIn("未知", unknown["error"])

            with patch.object(service, "_dispatch_resume_poll") as dispatch:
                await service.start()
            dispatch.assert_called_once()
            self.assertEqual(dispatch.call_args.args[1], "conversation-1")


if __name__ == "__main__":
    unittest.main()
